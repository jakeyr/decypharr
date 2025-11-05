package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/sirrobot01/decypharr/internal/config"
	"golang.org/x/crypto/bcrypt"
)

// SetupState tracks the current setup wizard state
type SetupState struct {
	Completed      bool   `json:"completed"`
	CurrentStep    int    `json:"current_step"`
	Username       string `json:"username,omitempty"`
	DebridProvider string `json:"debrid_provider,omitempty"`
	DebridAPIKey   string `json:"debrid_api_key,omitempty"`
	MountFolder    string `json:"mount_folder,omitempty"`
	DownloadFolder string `json:"download_folder,omitempty"`
	MountSystem    string `json:"mount_system,omitempty"` // "dfs" or "rclone"
	MountPath      string `json:"mount_path,omitempty"`
	CacheDir       string `json:"cache_dir,omitempty"`
}

// SetupWizardRequest represents a request from the setup wizard
type SetupWizardRequest struct {
	Step int                    `json:"step"`
	Data map[string]interface{} `json:"data"`
}

// SetupWizardResponse represents the response from setup wizard
type SetupWizardResponse struct {
	Success      bool        `json:"success"`
	Message      string      `json:"message,omitempty"`
	Error        string      `json:"error,omitempty"`
	NextStep     int         `json:"next_step,omitempty"`
	State        *SetupState `json:"state,omitempty"`
	Validation   interface{} `json:"validation,omitempty"`
	SetupNeeded  bool        `json:"setup_needed,omitempty"`
	RedirectTo   string      `json:"redirect_to,omitempty"`
	ConfigLoaded bool        `json:"config_loaded,omitempty"`
}

// SetupHandler renders the setup wizard page
func (s *Server) SetupHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()
	data := map[string]interface{}{
		"URLBase": cfg.URLBase,
		"Page":    "setup",
		"Title":   "Setup Wizard",
	}
	err := s.templates.ExecuteTemplate(w, "setup_layout", data)
	if err != nil {
		s.logger.Error().Err(err).Msg("template error")
	}
}

// setupStatusHandler checks if setup is needed
func (s *Server) setupStatusHandler(w http.ResponseWriter, r *http.Request) {
	cfg := config.Get()

	// Check if setup wizard has been completed
	setupNeeded := !cfg.SetupCompleted

	response := SetupWizardResponse{
		Success:      true,
		SetupNeeded:  setupNeeded,
		ConfigLoaded: cfg.Path != "",
	}

	if setupNeeded {
		response.RedirectTo = "/setup"
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// setupGetStateHandler returns current setup state
func (s *Server) setupGetStateHandler(w http.ResponseWriter, r *http.Request) {
	// For the setup wizard, always start at step 1
	// The wizard saves progress in-memory during the flow, not to config
	state := &SetupState{
		CurrentStep: 1,
		Completed:   false,
	}

	response := SetupWizardResponse{
		Success: true,
		State:   state,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// setupSaveStepHandler saves data for a specific step
func (s *Server) setupSaveStepHandler(w http.ResponseWriter, r *http.Request) {
	var req SetupWizardRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendSetupError(w, "Invalid request format", err)
		return
	}

	cfg := config.Get()
	response := SetupWizardResponse{Success: true}

	switch req.Step {
	case 0:
		// Step 0: Skip Setup
		response = s.handleSkipSetup(cfg, req.Data)

	case 1:
		// Step 1: Authentication
		response = s.handleAuthSetup(cfg, req.Data)

	case 2:
		// Step 2: Debrid Account
		response = s.handleDebridSetup(cfg, req.Data)

	case 3:
		// Step 3: Download Folder
		response = s.handleDownloadFolderSetup(cfg, req.Data)

	case 4:
		// Step 4: Mount System
		response = s.handleMountSetup(cfg, req.Data)

	case 5:
		// Step 5: Finalize
		response = s.handleSetupFinalize(cfg, req.Data)

	default:
		s.sendSetupError(w, "Invalid step", fmt.Errorf("step %d not found", req.Step))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleSkipSetup handles step 0: Skip setup for users with existing config
func (s *Server) handleSkipSetup(cfg *config.Config, data map[string]interface{}) SetupWizardResponse {
	skipSetup, _ := data["skip_setup"].(bool)

	if !skipSetup {
		return SetupWizardResponse{
			Success: false,
			Error:   "Invalid skip setup request",
		}
	}

	// Mark setup as completed without modifying config
	cfg.SetupCompleted = true
	if err := cfg.Save(); err != nil {
		return SetupWizardResponse{
			Success: false,
			Error:   "Failed to save configuration: " + err.Error(),
		}
	}

	return SetupWizardResponse{
		Success:    true,
		Message:    "Setup skipped successfully",
		RedirectTo: "/",
	}
}

// handleAuthSetup handles step 1: Authentication setup
func (s *Server) handleAuthSetup(cfg *config.Config, data map[string]interface{}) SetupWizardResponse {
	username, _ := data["username"].(string)
	password, _ := data["password"].(string)
	skipAuth, _ := data["skip_auth"].(bool)

	if skipAuth {
		cfg.UseAuth = false
		cfg.Username = ""
		cfg.Password = ""
	} else {
		if username == "" || password == "" {
			return SetupWizardResponse{
				Success: false,
				Error:   "Username and password are required",
			}
		}

		// Setup authentication using existing auth system
		auth := cfg.GetAuth()
		if auth == nil {
			auth = &config.Auth{}
		}
		auth.Username = username

		// Hash password using bcrypt
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
		if err != nil {
			return SetupWizardResponse{
				Success: false,
				Error:   "Failed to hash password",
			}
		}
		auth.Password = string(hashedPassword)

		cfg.UseAuth = true
		if err := cfg.SaveAuth(auth); err != nil {
			return SetupWizardResponse{
				Success: false,
				Error:   "Failed to save authentication: " + err.Error(),
			}
		}
	}

	if err := cfg.Save(); err != nil {
		return SetupWizardResponse{
			Success: false,
			Error:   "Failed to save configuration: " + err.Error(),
		}
	}

	return SetupWizardResponse{
		Success:  true,
		Message:  "Authentication configured successfully",
		NextStep: 2,
	}
}

// handleDebridSetup handles step 2: Debrid account setup
func (s *Server) handleDebridSetup(cfg *config.Config, data map[string]interface{}) SetupWizardResponse {
	provider, _ := data["provider"].(string)
	apiKey, _ := data["api_key"].(string)
	downloadAPIKey, _ := data["download_api_key"].(string)
	mountFolder, _ := data["mount_folder"].(string)

	// Validation
	validProviders := map[string]bool{
		"realdebrid": true,
		"alldebrid":  true,
		"debridlink": true,
		"torbox":     true,
	}

	if !validProviders[provider] {
		return SetupWizardResponse{
			Success: false,
			Error:   "Invalid debrid provider. Choose: realdebrid, alldebrid, debridlink, or torbox",
		}
	}

	if apiKey == "" {
		return SetupWizardResponse{
			Success: false,
			Error:   "API key is required",
		}
	}

	if mountFolder == "" {
		// Set default mount folder
		mountFolder = filepath.Join(cfg.Path, "mounts", provider, "__all__")
	}

	// Use download API key if provided, otherwise use main API key
	if downloadAPIKey == "" {
		downloadAPIKey = apiKey
	}

	// Create or update debrid config
	debrid := config.Debrid{
		Provider:        provider,
		Name:            provider,
		APIKey:          apiKey,
		DownloadAPIKeys: []string{downloadAPIKey},
		Folder:          mountFolder,
		// Set sensible defaults
		DownloadUncached:             false,
		CheckCached:                  true,
		RateLimit:                    "200/minute",
		RepairRateLimit:              "10/minute",
		DownloadRateLimit:            "200/minute",
		UnpackRar:                    false,
		AddSamples:                   false,
		MinimumFreeSlot:              1,
		TorrentsRefreshInterval:      "45s",
		DownloadLinksRefreshInterval: "40m",
		Workers:                      5,
		AutoExpireLinksAfter:         "90m",
		ServeFromRclone:              false,
		FolderNaming:                 "original_filename",
	}

	// Replace or add debrid config
	if len(cfg.Debrids) == 0 {
		cfg.Debrids = []config.Debrid{debrid}
	} else {
		cfg.Debrids[0] = debrid
	}

	if err := cfg.Save(); err != nil {
		return SetupWizardResponse{
			Success: false,
			Error:   "Failed to save configuration: " + err.Error(),
		}
	}

	return SetupWizardResponse{
		Success:  true,
		Message:  "Debrid account configured successfully",
		NextStep: 3,
	}
}

// handleDownloadFolderSetup handles step 3: Download folder setup
func (s *Server) handleDownloadFolderSetup(cfg *config.Config, data map[string]interface{}) SetupWizardResponse {
	downloadFolder, _ := data["download_folder"].(string)

	if downloadFolder == "" {
		// Set default
		downloadFolder = filepath.Join(cfg.Path, "downloads")
	}

	// Create the folder if it doesn't exist
	if err := os.MkdirAll(downloadFolder, 0755); err != nil {
		return SetupWizardResponse{
			Success: false,
			Error:   "Failed to create download folder: " + err.Error(),
		}
	}

	// Update Manager config (new) and QBitTorrent (deprecated, for compatibility)
	cfg.Manager.DownloadFolder = downloadFolder
	cfg.QBitTorrent.DownloadFolder = downloadFolder

	// Set other Manager defaults if not set
	if len(cfg.Manager.Categories) == 0 {
		cfg.Manager.Categories = []string{"sonarr", "radarr"}
	}
	if cfg.Manager.MaxDownloads == 0 {
		cfg.Manager.MaxDownloads = 10
	}

	if err := cfg.Save(); err != nil {
		return SetupWizardResponse{
			Success: false,
			Error:   "Failed to save configuration: " + err.Error(),
		}
	}

	return SetupWizardResponse{
		Success:  true,
		Message:  "Download folder configured successfully",
		NextStep: 4,
	}
}

// handleMountSetup handles step 4: Mount system setup
func (s *Server) handleMountSetup(cfg *config.Config, data map[string]interface{}) SetupWizardResponse {
	mountSystem, _ := data["mount_system"].(string)
	mountPath, _ := data["mount_path"].(string)
	cacheDir, _ := data["cache_dir"].(string)

	if mountSystem != "dfs" && mountSystem != "rclone" {
		return SetupWizardResponse{
			Success: false,
			Error:   "Invalid mount system. Choose 'dfs' or 'rclone'",
		}
	}

	if mountPath == "" {
		mountPath = filepath.Join(cfg.Path, "mounts")
	}

	if mountSystem == "dfs" {
		// Configure DFS
		cfg.Dfs.Enabled = true
		cfg.Dfs.MountPath = mountPath
		cfg.Rclone.Enabled = false

		if cacheDir == "" {
			cacheDir = filepath.Join(cfg.Path, "cache", "dfs")
		}

		// Create cache dir
		if err := os.MkdirAll(cacheDir, 0755); err != nil {
			return SetupWizardResponse{
				Success: false,
				Error:   "Failed to create cache directory: " + err.Error(),
			}
		}

		cfg.Dfs.CacheDir = cacheDir

		// Set sensible DFS defaults
		if cfg.Dfs.ChunkSize == "" {
			cfg.Dfs.ChunkSize = "8MB"
		}
		if cfg.Dfs.ReadAheadSize == "" {
			cfg.Dfs.ReadAheadSize = "32MB"
		}
		if cfg.Dfs.CacheExpiry == "" {
			cfg.Dfs.CacheExpiry = "24h"
		}
		if cfg.Dfs.AttrTimeout == "" {
			cfg.Dfs.AttrTimeout = "1m"
		}
		if cfg.Dfs.EntryTimeout == "" {
			cfg.Dfs.EntryTimeout = "1m"
		}

	} else {
		// Configure Rclone
		cfg.Rclone.Enabled = true
		cfg.Rclone.MountPath = mountPath
		cfg.Dfs.Enabled = false

		// Set sensible Rclone defaults
		if cfg.Rclone.VfsCacheMode == "" {
			cfg.Rclone.VfsCacheMode = "full"
		}
		if cfg.Rclone.VfsReadChunkSize == "" {
			cfg.Rclone.VfsReadChunkSize = "128M"
		}
		if cfg.Rclone.BufferSize == "" {
			cfg.Rclone.BufferSize = "128M"
		}
		if cfg.Rclone.DirCacheTime == "" {
			cfg.Rclone.DirCacheTime = "5m"
		}
	}

	if err := cfg.Save(); err != nil {
		return SetupWizardResponse{
			Success: false,
			Error:   "Failed to save configuration: " + err.Error(),
		}
	}

	return SetupWizardResponse{
		Success:  true,
		Message:  "Mount system configured successfully",
		NextStep: 5,
	}
}

// handleSetupFinalize handles step 5: Finalize and restart
func (s *Server) handleSetupFinalize(cfg *config.Config, data map[string]interface{}) SetupWizardResponse {
	// Final validation
	if len(cfg.Debrids) == 0 {
		return SetupWizardResponse{
			Success: false,
			Error:   "No debrid account configured",
		}
	}

	if cfg.Manager.DownloadFolder == "" {
		return SetupWizardResponse{
			Success: false,
			Error:   "Download folder not configured",
		}
	}

	if !cfg.Dfs.Enabled && !cfg.Rclone.Enabled {
		return SetupWizardResponse{
			Success: false,
			Error:   "No mount system configured",
		}
	}

	// Set setup as completed
	cfg.SetupCompleted = true

	if err := cfg.Save(); err != nil {
		return SetupWizardResponse{
			Success: false,
			Error:   "Failed to save final configuration: " + err.Error(),
		}
	}

	// Trigger manager restart to apply new config
	go s.Restart()

	return SetupWizardResponse{
		Success:    true,
		Message:    "Setup completed successfully! Restarting services...",
		RedirectTo: "/",
	}
}

// sendSetupError sends an error response
func (s *Server) sendSetupError(w http.ResponseWriter, message string, err error) {
	response := SetupWizardResponse{
		Success: false,
		Error:   message,
	}
	if err != nil {
		response.Error = fmt.Sprintf("%s: %v", message, err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	json.NewEncoder(w).Encode(response)
}

// setupValidateHandler validates configuration at each step
func (s *Server) setupValidateHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Step int                    `json:"step"`
		Data map[string]interface{} `json:"data"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendSetupError(w, "Invalid request", err)
		return
	}

	validation := make(map[string]interface{})

	switch req.Step {
	case 2:
		// Validate debrid provider
		provider, _ := req.Data["provider"].(string)
		apiKey, _ := req.Data["api_key"].(string)

		if provider != "" && apiKey != "" {
			// Test API key validity (optional, can be slow)
			validation["provider_valid"] = true
			validation["api_key_format_valid"] = len(apiKey) > 10
		}

	case 3:
		// Validate download folder
		folder, _ := req.Data["download_folder"].(string)
		if folder != "" {
			_, err := os.Stat(folder)
			validation["folder_exists"] = err == nil
			validation["folder_writable"] = true // Could test write permission
		}

	case 4:
		// Validate mount path
		mountPath, _ := req.Data["mount_path"].(string)
		if mountPath != "" {
			validation["mount_path_valid"] = true
		}
	}

	response := SetupWizardResponse{
		Success:    true,
		Validation: validation,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// SetupCompleteRequest represents the complete setup data from frontend
type SetupCompleteRequest struct {
	Auth struct {
		Username string `json:"username,omitempty"`
		Password string `json:"password,omitempty"`
		SkipAuth bool   `json:"skip_auth,omitempty"`
	} `json:"auth"`
	Debrid struct {
		Provider    string `json:"provider"`
		APIKey      string `json:"api_key"`
		DownloadKey string `json:"download_key,omitempty"`
		MountFolder string `json:"mount_folder"`
	} `json:"debrid"`
	Download struct {
		DownloadFolder string `json:"download_folder"`
	} `json:"download"`
	Mount struct {
		MountType        string `json:"mount_type"`
		MountPath        string `json:"mount_path"`
		CacheDir         string `json:"cache_dir"`
		RcloneBufferSize string `json:"rclone_buffer_size,omitempty"`
	} `json:"mount"`
}

// setupCompleteHandler handles the complete setup in a single request
func (s *Server) setupCompleteHandler(w http.ResponseWriter, r *http.Request) {
	var req SetupCompleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendSetupError(w, "Invalid request format", err)
		return
	}

	cfg := config.Get()

	// Step 1: Handle Authentication
	if req.Auth.SkipAuth {
		cfg.UseAuth = false
		cfg.Username = ""
		cfg.Password = ""
	} else if req.Auth.Username != "" && req.Auth.Password != "" {
		auth := cfg.GetAuth()
		if auth == nil {
			auth = &config.Auth{}
		}
		auth.Username = req.Auth.Username

		// Hash password using bcrypt
		hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Auth.Password), bcrypt.DefaultCost)
		if err != nil {
			s.sendSetupError(w, "Failed to hash password", err)
			return
		}
		auth.Password = string(hashedPassword)

		cfg.UseAuth = true
		if err := cfg.SaveAuth(auth); err != nil {
			s.sendSetupError(w, "Failed to save authentication", err)
			return
		}
	}

	// Step 2: Handle Debrid Account
	if req.Debrid.Provider == "" || req.Debrid.APIKey == "" || req.Debrid.MountFolder == "" {
		s.sendSetupError(w, "Debrid provider, API key, and mount folder are required", nil)
		return
	}

	validProviders := map[string]bool{
		"realdebrid": true,
		"alldebrid":  true,
		"debridlink": true,
		"torbox":     true,
	}

	if !validProviders[req.Debrid.Provider] {
		s.sendSetupError(w, "Invalid debrid provider", nil)
		return
	}

	downloadKey := req.Debrid.DownloadKey
	if downloadKey == "" {
		downloadKey = req.Debrid.APIKey
	}

	debrid := config.Debrid{
		Provider:                     req.Debrid.Provider,
		Name:                         req.Debrid.Provider,
		APIKey:                       req.Debrid.APIKey,
		DownloadAPIKeys:              []string{downloadKey},
		Folder:                       req.Debrid.MountFolder,
		DownloadUncached:             false,
		CheckCached:                  true,
		RateLimit:                    "200/minute",
		RepairRateLimit:              "10/minute",
		DownloadRateLimit:            "200/minute",
		UnpackRar:                    false,
		AddSamples:                   false,
		MinimumFreeSlot:              1,
		TorrentsRefreshInterval:      "45s",
		DownloadLinksRefreshInterval: "40m",
		Workers:                      5,
		AutoExpireLinksAfter:         "90m",
		ServeFromRclone:              false,
		FolderNaming:                 "original_filename",
	}

	if len(cfg.Debrids) == 0 {
		cfg.Debrids = []config.Debrid{debrid}
	} else {
		cfg.Debrids[0] = debrid
	}

	// Step 3: Handle Download Folder
	if req.Download.DownloadFolder == "" {
		s.sendSetupError(w, "Download folder is required", nil)
		return
	}

	// Create the folder if it doesn't exist
	if err := os.MkdirAll(req.Download.DownloadFolder, 0755); err != nil {
		s.sendSetupError(w, "Failed to create download folder", err)
		return
	}

	cfg.Manager.DownloadFolder = req.Download.DownloadFolder
	cfg.QBitTorrent.DownloadFolder = req.Download.DownloadFolder

	// Set Manager defaults if not set
	if len(cfg.Manager.Categories) == 0 {
		cfg.Manager.Categories = []string{"sonarr", "radarr"}
	}
	if cfg.Manager.MaxDownloads == 0 {
		cfg.Manager.MaxDownloads = 10
	}

	// Step 4: Handle Mount System
	if req.Mount.MountType != "dfs" && req.Mount.MountType != "rclone" {
		s.sendSetupError(w, "Invalid mount system. Choose 'dfs' or 'rclone'", nil)
		return
	}

	if req.Mount.MountPath == "" {
		s.sendSetupError(w, "Mount path is required", nil)
		return
	}

	if req.Mount.CacheDir == "" {
		s.sendSetupError(w, "Cache directory is required", nil)
		return
	}

	if req.Mount.MountType == "dfs" {
		// Configure DFS
		cfg.Dfs.Enabled = true
		cfg.Dfs.MountPath = req.Mount.MountPath
		cfg.Rclone.Enabled = false

		// Create cache dir
		if err := os.MkdirAll(req.Mount.CacheDir, 0755); err != nil {
			s.sendSetupError(w, "Failed to create cache directory", err)
			return
		}

		cfg.Dfs.CacheDir = req.Mount.CacheDir

		// Set sensible DFS defaults
		if cfg.Dfs.ChunkSize == "" {
			cfg.Dfs.ChunkSize = "8MB"
		}
		if cfg.Dfs.ReadAheadSize == "" {
			cfg.Dfs.ReadAheadSize = "32MB"
		}
		if cfg.Dfs.CacheExpiry == "" {
			cfg.Dfs.CacheExpiry = "24h"
		}
		if cfg.Dfs.AttrTimeout == "" {
			cfg.Dfs.AttrTimeout = "1m"
		}
		if cfg.Dfs.EntryTimeout == "" {
			cfg.Dfs.EntryTimeout = "1m"
		}

	} else {
		// Configure Rclone
		cfg.Rclone.Enabled = true
		cfg.Rclone.MountPath = req.Mount.MountPath
		cfg.Dfs.Enabled = false

		if req.Mount.CacheDir != "" {
			cfg.Rclone.CacheDir = req.Mount.CacheDir
		}

		// Set sensible Rclone defaults
		if cfg.Rclone.VfsCacheMode == "" {
			cfg.Rclone.VfsCacheMode = "full"
		}
		if cfg.Rclone.VfsReadChunkSize == "" {
			cfg.Rclone.VfsReadChunkSize = "128M"
		}
		if req.Mount.RcloneBufferSize != "" {
			cfg.Rclone.BufferSize = req.Mount.RcloneBufferSize
		} else if cfg.Rclone.BufferSize == "" {
			cfg.Rclone.BufferSize = "128M"
		}
		if cfg.Rclone.DirCacheTime == "" {
			cfg.Rclone.DirCacheTime = "5m"
		}
	}

	// Set setup as completed
	cfg.SetupCompleted = true

	if err := cfg.Save(); err != nil {
		s.sendSetupError(w, "Failed to save configuration", err)
		return
	}

	// Trigger manager restart to apply new config
	go s.Restart()

	response := SetupWizardResponse{
		Success:    true,
		Message:    "Setup completed successfully! Restarting services...",
		RedirectTo: "/",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// setupSkipHandler handles skipping the setup wizard
func (s *Server) setupSkipHandler(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Skip bool `json:"skip"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		s.sendSetupError(w, "Invalid request format", err)
		return
	}

	if !req.Skip {
		s.sendSetupError(w, "Invalid skip setup request", nil)
		return
	}

	cfg := config.Get()
	cfg.SetupCompleted = true
	if err := cfg.Save(); err != nil {
		s.sendSetupError(w, "Failed to save configuration", err)
		return
	}

	response := SetupWizardResponse{
		Success:    true,
		Message:    "Setup skipped successfully",
		RedirectTo: "/",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}
