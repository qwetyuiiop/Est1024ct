// main.go - 控制中心最终版（含文件传输）
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"embed"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"html/template"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/robfig/cron/v3"
	"golang.org/x/time/rate"
)

//go:embed static/*
var static embed.FS

// ==================== 常量定义 ====================

const (
	maxLogs          = 1000
	writeTimeout     = 10 * time.Second
	pingInterval     = 30 * time.Second
	pingTimeout      = 60 * time.Second
	defaultPort      = "8080"
	maxPayloadLen    = 10 * 1024
	maxTaskName      = 100
	maxTaskID        = 64
	rateLimitPerSec  = 1
	rateLimitBurst   = 10
	cookieName       = "ctrl_token"
	cookieMaxAge     = 86400
	maxFileSize      = 100 * 1024 * 1024 // 100MB
	chunkSize        = 256 * 1024        // 256KB
)

// ==================== 数据结构 ====================

type Config struct {
	Port      string
	Token     string
	Secret    string
	StaticDir string
	Debug     bool
}

type APIResponse struct {
	Success bool        `json:"success"`
	Data    interface{} `json:"data,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type Task struct {
	ID          string      `json:"id"`
	Name        string      `json:"name"`
	Schedule    string      `json:"schedule"`
	Weekdays    string      `json:"weekdays"`
	Type        string      `json:"type"`
	Payload     string      `json:"payload,omitempty"`
	DesktopGUID string      `json:"desktop_guid,omitempty"`
	Config      *TaskConfig `json:"config,omitempty"`
	Enabled     bool        `json:"enabled"`
	CreatedAt   time.Time   `json:"created_at"`
}

type TaskConfig struct {
	WSURL string `json:"ws_url"`
	Token string `json:"token"`
}

type Command struct {
	Type        string      `json:"type"`
	Payload     string      `json:"payload,omitempty"`
	DesktopGUID string      `json:"desktop_guid,omitempty"`
	Token       string      `json:"token"`
	Config      *TaskConfig `json:"config,omitempty"`
}

type Report struct {
	Type      string    `json:"type"`
	Payload   string    `json:"payload"`
	From      string    `json:"from,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

type Client struct {
	ID        string
	Conn      *websocket.Conn
	LastSeen  time.Time
	WriteMu   sync.Mutex
	closeOnce sync.Once
	closed    atomic.Bool
}

type FileTransfer struct {
	ID             string    `json:"id"`
	Filename       string    `json:"filename"`
	Size           int64     `json:"size"`
	ChunkSize      int       `json:"chunk_size"`
	TotalChunks    int       `json:"total_chunks"`
	SentChunks     int       `json:"sent_chunks"`
	TargetPath     string    `json:"target_path"`
	TargetClient   string    `json:"target_client"`
	Status         string    `json:"status"`
	Error          string    `json:"error,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type FileChunkMeta struct {
	TransferID  string `json:"transfer_id"`
	ChunkIndex  int    `json:"chunk_index"`
	TotalChunks int    `json:"total_chunks"`
	Filename    string `json:"filename"`
	Size        int64  `json:"size"`
	TargetPath  string `json:"target_path"`
	ChunkSize   int    `json:"chunk_size"`
}

// ==================== 全局变量 ====================

var (
	cfg Config
	
	upgrader = websocket.Upgrader{
		CheckOrigin:     checkOrigin,
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
	
	clients     = make(map[string]*Client)
	clientsMu   sync.RWMutex
	nextClientID uint64
	
	tasks        = make(map[string]*Task)
	tasksMu      sync.RWMutex
	taskFile     = "tasks.json"
	taskEntries  = make(map[string]cron.EntryID)
	taskScheduler *cron.Cron
	taskSaveMu   sync.Mutex
	
	logs     [maxLogs]Report
	logIndex int
	logCount int
	logsMu   sync.Mutex
	
	fileTransfers     = make(map[string]*FileTransfer)
	fileTransfersMu   sync.RWMutex
	transferFile      = "transfers.json"
	nextTransferID    uint64
	
	rateLimiter = rate.NewLimiter(rate.Limit(rateLimitPerSec), rateLimitBurst)
	
	stopCh = make(chan struct{})
)

// ==================== 主函数 ====================

func main() {
	port := flag.String("port", getEnv("PORT", defaultPort), "listen port")
	token := flag.String("token", getEnv("TOKEN", ""), "auth token (required)")
	secret := flag.String("secret", getEnv("SECRET", ""), "session secret (required)")
	debug := flag.Bool("debug", getEnvBool("DEBUG", false), "debug mode")
	flag.Parse()
	
	if *token == "" {
		log.Fatal("必须提供 -token 参数或设置 TOKEN 环境变量")
	}
	if *secret == "" {
		log.Fatal("必须提供 -secret 参数或设置 SECRET 环境变量")
	}
	
	cfg = Config{
		Port:   *port,
		Token:  *token,
		Secret: *secret,
		Debug:  *debug,
	}
	
	if !cfg.Debug {
		gin.SetMode(gin.ReleaseMode)
	}
	
	loadTasks()
	loadTransfers()
	
	taskScheduler = cron.New(cron.WithSeconds(), cron.WithLocation(time.UTC))
	taskScheduler.Start()
	defer taskScheduler.Stop()
	rescheduleAllTasks()
	
	r := gin.New()
	r.Use(gin.Recovery())
	if cfg.Debug {
		r.Use(gin.Logger())
	}
	r.Use(httpsMiddleware())
	r.Use(rateLimitMiddleware())
	r.Use(bodySizeMiddleware())
	
	r.GET("/health", healthCheck)
	r.GET("/login", loginPage)
	r.POST("/login", doLogin)
	r.GET("/logout", logout)
	
	authGroup := r.Group("/")
	authGroup.Use(cookieAuthMiddleware())
	{
		staticFS, _ := fs.Sub(static, "static")
		authGroup.StaticFS("/static", http.FS(staticFS))
		authGroup.GET("/", func(c *gin.Context) {
			c.Redirect(302, "/static/index.html")
		})
	}
	
	api := r.Group("/api/v1")
	api.Use(cookieAuthMiddleware())
	{
		api.GET("/tasks", getTasks)
		api.POST("/tasks", createTask)
		api.DELETE("/tasks/:id", deleteTask)
		api.PATCH("/tasks/:id/toggle", toggleTask)
		api.POST("/tasks/:id/run", runTaskNow)
		api.GET("/clients", getClients)
		api.POST("/broadcast", broadcastCommand)
		api.GET("/logs", getLogs)
		api.GET("/token", getTokenInfo)
		
		// 文件传输
		api.POST("/upload", uploadFile)
		api.GET("/transfers", getFileTransfers)
		api.DELETE("/transfers/:id", cancelFileTransfer)
	}
	
	r.GET("/ws", wsHandler)
	
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("收到退出信号，正在关闭...")
		close(stopCh)
		time.Sleep(2 * time.Second)
		saveTransfers()
		os.Exit(0)
	}()
	
	log.Printf("控制中心启动于 :%s", cfg.Port)
	log.Printf("登录地址: http://localhost:%s/login", cfg.Port)
	log.Printf("默认Token: %s", cfg.Token)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatal("启动失败:", err)
	}
}

// ==================== 辅助函数 ====================

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return strings.ToLower(value) == "true" || value == "1"
	}
	return defaultValue
}

func generateSessionID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func hashSecret(secret, sessionID string) string {
	h := sha256.Sum256([]byte(secret + sessionID))
	return hex.EncodeToString(h[:])
}

func checkOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	
	allowed := []string{
		"http://localhost:" + cfg.Port,
		"http://127.0.0.1:" + cfg.Port,
		"https://" + r.Host,
	}
	
	for _, a := range allowed {
		if origin == a {
			return true
		}
	}
	
	if cfg.Debug {
		return true
	}
	
	return false
}

func validateSchedule(schedule string) error {
	var hour, minute int
	n, err := fmt.Sscanf(schedule, "%d:%d", &hour, &minute)
	if err != nil || n != 2 {
		return fmt.Errorf("格式错误，应为 HH:MM")
	}
	if hour < 0 || hour > 23 {
		return fmt.Errorf("小时必须在0-23之间")
	}
	if minute < 0 || minute > 59 {
		return fmt.Errorf("分钟必须在0-59之间")
	}
	return nil
}

func validateWeekdays(weekdays string) error {
	if weekdays == "" {
		return nil
	}
	for _, w := range strings.Split(weekdays, ",") {
		var d int
		if _, err := fmt.Sscanf(w, "%d", &d); err != nil {
			return fmt.Errorf("无效星期: %s", w)
		}
		if d < 0 || d > 6 {
			return fmt.Errorf("星期必须在0-6之间")
		}
	}
	return nil
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}

func redactToken(s string) string {
	if cfg.Token != "" && strings.Contains(s, cfg.Token) {
		s = strings.ReplaceAll(s, cfg.Token, "[REDACTED]")
	}
	return s
}

// ==================== 中间件 ====================

func cookieAuthMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		cookie, err := c.Cookie(cookieName)
		if err != nil || cookie == "" {
			c.Redirect(302, "/login")
			c.Abort()
			return
		}
		
		parts := strings.Split(cookie, ":")
		if len(parts) != 2 {
			c.Redirect(302, "/login")
			c.Abort()
			return
		}
		
		sessionID, hash := parts[0], parts[1]
		expectedHash := hashSecret(cfg.Secret, sessionID)
		if hash != expectedHash {
			c.Redirect(302, "/login")
			c.Abort()
			return
		}
		
		c.Next()
	}
}

func rateLimitMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.URL.Path == "/health" || c.Request.URL.Path == "/login" {
			c.Next()
			return
		}
		if !rateLimiter.Allow() {
			c.JSON(429, APIResponse{Success: false, Error: "请求过于频繁，请稍后再试"})
			c.Abort()
			return
		}
		c.Next()
	}
}

func httpsMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if cfg.Debug || cfg.Port == "8080" || cfg.Port == "3000" {
			c.Next()
			return
		}
		if c.Request.TLS == nil && c.GetHeader("X-Forwarded-Proto") != "https" {
			url := "https://" + c.Request.Host + c.Request.URL.String()
			c.Redirect(301, url)
			c.Abort()
			return
		}
		c.Next()
	}
}

func bodySizeMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxPayloadLen)
		c.Next()
	}
}

// ==================== 登录处理 ====================

func loginPage(c *gin.Context) {
	c.Header("Content-Type", "text/html; charset=utf-8")
	html := `<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>控制中心登录</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); min-height: 100vh; display: flex; align-items: center; justify-content: center; }
        .login-card { background: white; border-radius: 20px; box-shadow: 0 20px 40px rgba(0,0,0,0.2); padding: 40px; width: 100%; max-width: 400px; }
        .login-card h2 { margin-bottom: 30px; color: #333; }
        .btn-primary { background: linear-gradient(90deg, #667eea, #764ba2); border: none; width: 100%; padding: 12px; }
        .token-info { font-size: 12px; color: #666; margin-top: 20px; text-align: center; word-break: break-all; }
    </style>
</head>
<body>
    <div class="login-card">
        <h2 class="text-center">任务控制中心</h2>
        <form method="POST" action="/login">
            <div class="mb-3">
                <label class="form-label">Token</label>
                <input type="password" class="form-control" name="token" required autofocus>
                <div class="form-text">请输入管理员Token</div>
            </div>
            <button type="submit" class="btn btn-primary">登录</button>
        </form>
        <div class="token-info">
            <small>默认Token: <code>` + cfg.Token + `</code></small>
        </div>
    </div>
</body>
</html>`
	c.String(200, html)
}

func doLogin(c *gin.Context) {
	token := c.PostForm("token")
	if token != cfg.Token {
		c.String(401, "Token错误")
		return
	}
	
	sessionID := generateSessionID()
	hash := hashSecret(cfg.Secret, sessionID)
	cookie := sessionID + ":" + hash
	
	c.SetCookie(cookieName, cookie, cookieMaxAge, "/", "", false, true)
	c.Redirect(302, "/")
}

func logout(c *gin.Context) {
	c.SetCookie(cookieName, "", -1, "/", "", false, true)
	c.Redirect(302, "/login")
}

func getTokenInfo(c *gin.Context) {
	c.JSON(200, APIResponse{
		Success: true,
		Data: map[string]string{
			"token": cfg.Token,
		},
	})
}

func healthCheck(c *gin.Context) {
	c.JSON(200, APIResponse{
		Success: true,
		Data: map[string]interface{}{
			"status":  "ok",
			"clients": len(clients),
			"tasks":   len(tasks),
		},
	})
}

// ==================== WebSocket处理 ====================

func wsHandler(c *gin.Context) {
	clientToken := ""
	if protocols := c.Request.Header["Sec-WebSocket-Protocol"]; len(protocols) > 0 {
		clientToken = protocols[0]
	}
	if clientToken == "" {
		clientToken = c.Query("token")
	}
	
	if clientToken != cfg.Token {
		c.JSON(401, APIResponse{Success: false, Error: "invalid token"})
		return
	}
	
	conn, err := upgrader.Upgrade(c.Writer, c.Request, http.Header{
		"Sec-WebSocket-Protocol": []string{cfg.Token},
	})
	if err != nil {
		log.Println("WebSocket升级失败:", err)
		return
	}
	
	clientID := fmt.Sprintf("client-%d", atomic.AddUint64(&nextClientID, 1))
	
	client := &Client{
		ID:       clientID,
		Conn:     conn,
		LastSeen: time.Now(),
	}
	
	clientsMu.Lock()
	clients[clientID] = client
	clientsMu.Unlock()
	
	log.Printf("客户端连接: %s (在线数: %d)", clientID, len(clients))
	
	conn.SetReadDeadline(time.Now().Add(pingTimeout))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(pingTimeout))
		client.LastSeen = time.Now()
		return nil
	})
	
	go func() {
		client.WriteJSON(map[string]string{"type": "welcome", "id": clientID})
		sendTasksToClient(client)
	}()
	
	go func() {
		ticker := time.NewTicker(pingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				if client.closed.Load() {
					return
				}
				if err := client.WriteJSON(map[string]string{"type": "ping"}); err != nil {
					return
				}
				client.LastSeen = time.Now()
			}
		}
	}()
	
	defer func() {
		client.Close()
		clientsMu.Lock()
		delete(clients, clientID)
		clientsMu.Unlock()
		log.Printf("客户端断开: %s (在线数: %d)", clientID, len(clients))
	}()
	
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			break
		}
		
		var report Report
		if err := json.Unmarshal(msg, &report); err == nil {
			report.From = clientID
			report.Timestamp = time.Now()
			report.Payload = redactToken(report.Payload)
			addLog(report)
			
			// 处理文件完成上报
			if report.Type == "file_complete" {
				var data map[string]string
				if err := json.Unmarshal([]byte(report.Payload), &data); err == nil {
					if transferID, ok := data["transfer_id"]; ok {
						fileTransfersMu.Lock()
						if t, ok := fileTransfers[transferID]; ok {
							t.Status = "completed"
						}
						fileTransfersMu.Unlock()
						saveTransfers()
					}
				}
			}
		}
		
		conn.SetReadDeadline(time.Now().Add(pingTimeout))
	}
}

func (c *Client) WriteJSON(v interface{}) error {
	if c.closed.Load() {
		return fmt.Errorf("connection closed")
	}
	c.WriteMu.Lock()
	defer c.WriteMu.Unlock()
	
	c.Conn.SetWriteDeadline(time.Now().Add(writeTimeout))
	return c.Conn.WriteJSON(v)
}

func (c *Client) Close() {
	c.closeOnce.Do(func() {
		c.closed.Store(true)
		c.Conn.Close()
	})
}

// ==================== 任务管理 ====================

func loadTasks() {
	data, err := os.ReadFile(taskFile)
	if err != nil {
		return
	}
	var taskList []*Task
	if err := json.Unmarshal(data, &taskList); err != nil {
		log.Println("解析任务文件失败:", err)
		return
	}
	tasksMu.Lock()
	defer tasksMu.Unlock()
	for _, t := range taskList {
		tasks[t.ID] = t
	}
	log.Printf("加载 %d 个任务", len(tasks))
}

func saveTasks() {
	taskSaveMu.Lock()
	defer taskSaveMu.Unlock()
	
	tasksMu.RLock()
	taskList := make([]*Task, 0, len(tasks))
	for _, t := range tasks {
		taskList = append(taskList, t)
	}
	tasksMu.RUnlock()
	
	data, err := json.MarshalIndent(taskList, "", "  ")
	if err != nil {
		log.Printf("序列化任务失败: %v", err)
		return
	}
	
	tmpFile := taskFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		log.Printf("写入临时文件失败: %v", err)
		return
	}
	if err := os.Rename(tmpFile, taskFile); err != nil {
		log.Printf("重命名失败: %v", err)
	}
}

func getCronExpr(task *Task) string {
	var hour, minute int
	fmt.Sscanf(task.Schedule, "%d:%d", &hour, &minute)
	
	weekday := "*"
	if task.Weekdays != "" && task.Weekdays != "0,1,2,3,4,5,6" {
		weekday = task.Weekdays
	}
	
	return fmt.Sprintf("0 %d %d * * %s", minute, hour, weekday)
}

func addTaskSchedule(task *Task) {
	if !task.Enabled {
		return
	}
	cronExpr := getCronExpr(task)
	entryID, err := taskScheduler.AddFunc(cronExpr, func() {
		log.Printf("定时任务触发: %s (%s)", task.Name, task.Schedule)
		cmd := taskToCommand(task)
		sendCommandToAll(cmd)
	})
	if err != nil {
		log.Printf("调度任务失败 %s: %v", task.ID, err)
		return
	}
	taskEntries[task.ID] = entryID
}

func removeTaskSchedule(taskID string) {
	if entryID, ok := taskEntries[taskID]; ok {
		taskScheduler.Remove(entryID)
		delete(taskEntries, taskID)
	}
}

func rescheduleAllTasks() {
	for _, entryID := range taskEntries {
		taskScheduler.Remove(entryID)
	}
	taskEntries = make(map[string]cron.EntryID)
	
	tasksMu.RLock()
	defer tasksMu.RUnlock()
	for _, task := range tasks {
		addTaskSchedule(task)
	}
}

func taskToCommand(task *Task) Command {
	cmd := Command{
		Type:        task.Type,
		Payload:     task.Payload,
		DesktopGUID: task.DesktopGUID,
		Token:       cfg.Token,
	}
	if task.Config != nil {
		cmd.Config = task.Config
	}
	return cmd
}

func sendTasksToClient(client *Client) {
	tasksMu.RLock()
	defer tasksMu.RUnlock()
	
	for _, task := range tasks {
		if task.Enabled {
			cmd := taskToCommand(task)
			client.WriteJSON(cmd)
		}
	}
}

func sendCommandToAll(cmd Command) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()
	
	for _, client := range clients {
		client.WriteJSON(cmd)
	}
}

// ==================== 文件传输 ====================

func loadTransfers() {
	data, err := os.ReadFile(transferFile)
	if err != nil {
		return
	}
	var transfers map[string]*FileTransfer
	if err := json.Unmarshal(data, &transfers); err != nil {
		log.Println("解析传输记录失败:", err)
		return
	}
	fileTransfersMu.Lock()
	defer fileTransfersMu.Unlock()
	for id, t := range transfers {
		fileTransfers[id] = t
	}
}

func saveTransfers() {
	fileTransfersMu.RLock()
	defer fileTransfersMu.RUnlock()
	
	data, err := json.MarshalIndent(fileTransfers, "", "  ")
	if err != nil {
		log.Printf("序列化传输记录失败: %v", err)
		return
	}
	
	tmpFile := transferFile + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0644); err != nil {
		log.Printf("写入临时文件失败: %v", err)
		return
	}
	os.Rename(tmpFile, transferFile)
}

func uploadFile(c *gin.Context) {
	target := c.Query("target")
	targetPath := c.PostForm("target_path")
	if targetPath == "" {
		targetPath = "C:\\Users\\Public\\Downloads"
	}
	
	// 安全检查
	targetPath = filepath.Clean(targetPath)
	if strings.Contains(targetPath, "..") {
		c.JSON(400, APIResponse{Success: false, Error: "无效的目标路径"})
		return
	}
	
	file, err := c.FormFile("file")
	if err != nil {
		c.JSON(400, APIResponse{Success: false, Error: "请选择文件"})
		return
	}
	
	if file.Size > maxFileSize {
		c.JSON(400, APIResponse{Success: false, Error: fmt.Sprintf("文件超过%dMB限制", maxFileSize/1024/1024)})
		return
	}
	
	src, err := file.Open()
	if err != nil {
		c.JSON(500, APIResponse{Success: false, Error: "打开文件失败"})
		return
	}
	defer src.Close()
	
	transferID := fmt.Sprintf("transfer-%d", atomic.AddUint64(&nextTransferID, 1))
	totalChunks := int((file.Size + int64(chunkSize) - 1) / int64(chunkSize))
	
	transfer := &FileTransfer{
		ID:           transferID,
		Filename:     filepath.Base(file.Filename),
		Size:         file.Size,
		ChunkSize:    chunkSize,
		TotalChunks:  totalChunks,
		SentChunks:   0,
		TargetPath:   targetPath,
		TargetClient: target,
		Status:       "pending",
		CreatedAt:    time.Now(),
	}
	
	fileTransfersMu.Lock()
	fileTransfers[transferID] = transfer
	fileTransfersMu.Unlock()
	saveTransfers()
	
	go func() {
		transfer.Status = "transferring"
		buffer := make([]byte, chunkSize)
		
		for chunkIndex := 0; chunkIndex < totalChunks; chunkIndex++ {
			n, err := src.Read(buffer)
			if err != nil && err != io.EOF {
				transfer.Status = "failed"
				transfer.Error = err.Error()
				saveTransfers()
				return
			}
			
			chunkData := buffer[:n]
			chunkBase64 := base64.StdEncoding.EncodeToString(chunkData)
			
			meta := FileChunkMeta{
				TransferID:  transferID,
				ChunkIndex:  chunkIndex,
				TotalChunks: totalChunks,
				Filename:    filepath.Base(file.Filename),
				Size:        file.Size,
				TargetPath:  targetPath,
				ChunkSize:   n,
			}
			metaJSON, _ := json.Marshal(meta)
			
			cmd := Command{
				Type:        "file_chunk",
				Payload:     chunkBase64,
				DesktopGUID: string(metaJSON),
				Token:       cfg.Token,
			}
			
			if target != "" {
				clientsMu.RLock()
				client, ok := clients[target]
				clientsMu.RUnlock()
				if ok {
					client.WriteJSON(cmd)
				} else {
					transfer.Status = "failed"
					transfer.Error = "目标客户端离线"
					saveTransfers()
					return
				}
			} else {
				sendCommandToAll(cmd)
			}
			
			fileTransfersMu.Lock()
			transfer.SentChunks = chunkIndex + 1
			fileTransfersMu.Unlock()
			saveTransfers()
			
			time.Sleep(10 * time.Millisecond)
		}
		
		transfer.Status = "completed"
		saveTransfers()
		log.Printf("文件传输完成: %s -> %s", file.Filename, targetPath)
	}()
	
	c.JSON(200, APIResponse{Success: true, Data: map[string]interface{}{
		"transfer_id":   transferID,
		"filename":      file.Filename,
		"size":          file.Size,
		"total_chunks":  totalChunks,
		"target_path":   targetPath,
	}})
}

func getFileTransfers(c *gin.Context) {
	fileTransfersMu.RLock()
	defer fileTransfersMu.RUnlock()
	
	transfers := make([]*FileTransfer, 0, len(fileTransfers))
	for _, t := range fileTransfers {
		transfers = append(transfers, t)
	}
	c.JSON(200, APIResponse{Success: true, Data: transfers})
}

func cancelFileTransfer(c *gin.Context) {
	id := c.Param("id")
	
	fileTransfersMu.Lock()
	transfer, ok := fileTransfers[id]
	if ok && transfer.Status == "transferring" {
		transfer.Status = "cancelled"
		saveTransfers()
	}
	fileTransfersMu.Unlock()
	
	if !ok {
		c.JSON(404, APIResponse{Success: false, Error: "传输任务不存在"})
		return
	}
	
	cmd := Command{
		Type:    "file_cancel",
		Payload: id,
		Token:   cfg.Token,
	}
	sendCommandToAll(cmd)
	
	c.JSON(200, APIResponse{Success: true, Data: map[string]string{"status": "cancelled"}})
}

// ==================== API处理 ====================

func getTasks(c *gin.Context) {
	tasksMu.RLock()
	defer tasksMu.RUnlock()
	
	taskList := make([]*Task, 0, len(tasks))
	for _, t := range tasks {
		taskList = append(taskList, t)
	}
	c.JSON(200, APIResponse{Success: true, Data: taskList})
}

func createTask(c *gin.Context) {
	var task Task
	if err := c.BindJSON(&task); err != nil {
		c.JSON(400, APIResponse{Success: false, Error: err.Error()})
		return
	}
	
	if task.ID == "" {
		c.JSON(400, APIResponse{Success: false, Error: "id required"})
		return
	}
	if len(task.ID) > maxTaskID {
		c.JSON(400, APIResponse{Success: false, Error: "id too long"})
		return
	}
	if task.Name == "" {
		c.JSON(400, APIResponse{Success: false, Error: "name required"})
		return
	}
	if len(task.Name) > maxTaskName {
		c.JSON(400, APIResponse{Success: false, Error: "name too long"})
		return
	}
	if err := validateSchedule(task.Schedule); err != nil {
		c.JSON(400, APIResponse{Success: false, Error: err.Error()})
		return
	}
	if err := validateWeekdays(task.Weekdays); err != nil {
		c.JSON(400, APIResponse{Success: false, Error: err.Error()})
		return
	}
	if len(task.Payload) > maxPayloadLen {
		c.JSON(400, APIResponse{Success: false, Error: "payload too large"})
		return
	}
	
	task.CreatedAt = time.Now()
	task.Enabled = true
	
	tasksMu.Lock()
	tasks[task.ID] = &task
	tasksMu.Unlock()
	
	addTaskSchedule(&task)
	saveTasks()
	
	c.JSON(200, APIResponse{Success: true, Data: task})
}

func deleteTask(c *gin.Context) {
	id := c.Param("id")
	
	removeTaskSchedule(id)
	
	tasksMu.Lock()
	delete(tasks, id)
	tasksMu.Unlock()
	
	saveTasks()
	c.JSON(200, APIResponse{Success: true, Data: map[string]string{"status": "deleted"}})
}

func toggleTask(c *gin.Context) {
	id := c.Param("id")
	
	tasksMu.Lock()
	task, ok := tasks[id]
	if ok {
		task.Enabled = !task.Enabled
	}
	tasksMu.Unlock()
	
	if !ok {
		c.JSON(404, APIResponse{Success: false, Error: "task not found"})
		return
	}
	
	if task.Enabled {
		addTaskSchedule(task)
	} else {
		removeTaskSchedule(id)
	}
	saveTasks()
	
	c.JSON(200, APIResponse{Success: true, Data: map[string]interface{}{
		"status":  "toggled",
		"enabled": task.Enabled,
	}})
}

func runTaskNow(c *gin.Context) {
	id := c.Param("id")
	
	tasksMu.RLock()
	task, ok := tasks[id]
	tasksMu.RUnlock()
	
	if !ok {
		c.JSON(404, APIResponse{Success: false, Error: "task not found"})
		return
	}
	
	cmd := taskToCommand(task)
	sendCommandToAll(cmd)
	
	c.JSON(200, APIResponse{Success: true, Data: map[string]string{"status": "executed", "task": task.Name}})
}

func getClients(c *gin.Context) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()
	
	clientList := make([]map[string]interface{}, 0, len(clients))
	for id, cl := range clients {
		clientList = append(clientList, map[string]interface{}{
			"id":        id,
			"last_seen": cl.LastSeen.Format(time.RFC3339),
		})
	}
	c.JSON(200, APIResponse{Success: true, Data: clientList})
}

func broadcastCommand(c *gin.Context) {
	var cmd Command
	if err := c.BindJSON(&cmd); err != nil {
		c.JSON(400, APIResponse{Success: false, Error: err.Error()})
		return
	}
	
	cmd.Token = cfg.Token
	
	target := c.Query("target")
	
	clientsMu.RLock()
	var targets []*Client
	if target != "" {
		if cl, ok := clients[target]; ok {
			targets = append(targets, cl)
		}
	} else {
		for _, cl := range clients {
			targets = append(targets, cl)
		}
	}
	clientsMu.RUnlock()
	
	success := 0
	for _, cl := range targets {
		if err := cl.WriteJSON(cmd); err == nil {
			success++
		}
	}
	
	c.JSON(200, APIResponse{Success: true, Data: map[string]interface{}{
		"target":  target,
		"success": success,
		"total":   len(targets),
	}})
}

func getLogs(c *gin.Context) {
	logsMu.Lock()
	defer logsMu.Unlock()
	
	result := make([]Report, 0, logCount)
	for i := 0; i < logCount; i++ {
		idx := (logIndex + i) % maxLogs
		result = append(result, logs[idx])
	}
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	c.JSON(200, APIResponse{Success: true, Data: result})
}

// ==================== 日志管理 ====================

func addLog(r Report) {
	logsMu.Lock()
	defer logsMu.Unlock()
	logs[logIndex] = r
	logIndex = (logIndex + 1) % maxLogs
	if logCount < maxLogs {
		logCount++
	}
}
