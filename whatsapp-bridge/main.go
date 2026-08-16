package main

import (
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal"

	"bytes"

	"go.mau.fi/util/random"
	"go.mau.fi/whatsmeow"
	waBinary "go.mau.fi/whatsmeow/binary"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	waCommon "go.mau.fi/whatsmeow/proto/waCommon"
	waCompanionReg "go.mau.fi/whatsmeow/proto/waCompanionReg"
	waE2E "go.mau.fi/whatsmeow/proto/waE2E"
	waHistorySync "go.mau.fi/whatsmeow/proto/waHistorySync"
	waMmsRetry "go.mau.fi/whatsmeow/proto/waMmsRetry"
	waWeb "go.mau.fi/whatsmeow/proto/waWeb"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"

	"rsc.io/qr"
)

// Message represents a chat message for our client
type Message struct {
	Time      time.Time
	Sender    string
	Content   string
	IsFromMe  bool
	MediaType string
	Filename  string
}

// Database handler for storing message history
type MessageStore struct {
	db *sql.DB
}

// Initialize message store
func NewMessageStore() (*MessageStore, error) {
	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		return nil, fmt.Errorf("failed to create store directory: %v", err)
	}

	// Open SQLite database for messages
	db, err := sql.Open("sqlite3", "file:store/messages.db?_foreign_keys=on")
	if err != nil {
		return nil, fmt.Errorf("failed to open message database: %v", err)
	}

	// Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS chats (
			jid TEXT PRIMARY KEY,
			name TEXT,
			last_message_time TIMESTAMP
		);
		
		CREATE TABLE IF NOT EXISTS messages (
			id TEXT,
			chat_jid TEXT,
			sender TEXT,
			content TEXT,
			timestamp TIMESTAMP,
			is_from_me BOOLEAN,
			media_type TEXT,
			filename TEXT,
			url TEXT,
			media_key BLOB,
			file_sha256 BLOB,
			file_enc_sha256 BLOB,
			file_length INTEGER,
			message_proto BLOB,
			PRIMARY KEY (id, chat_jid),
			FOREIGN KEY (chat_jid) REFERENCES chats(jid)
		);
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create tables: %v", err)
	}

	// Migration: add message_proto column if it doesn't exist (for existing DBs)
	_, _ = db.Exec("ALTER TABLE messages ADD COLUMN message_proto BLOB")

	return &MessageStore{db: db}, nil
}

// Close the database connection
func (store *MessageStore) Close() error {
	return store.db.Close()
}

// Store a chat in the database
func (store *MessageStore) StoreChat(jid, name string, lastMessageTime time.Time) error {
	_, err := store.db.Exec(
		"INSERT OR REPLACE INTO chats (jid, name, last_message_time) VALUES (?, ?, ?)",
		jid, name, lastMessageTime,
	)
	return err
}

// Store a message in the database
func (store *MessageStore) StoreMessage(id, chatJID, sender, content string, timestamp time.Time, isFromMe bool,
	mediaType, filename, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64, messageProto []byte) error {
	// Only store if there's actual content or media
	if content == "" && mediaType == "" {
		return nil
	}

	_, err := store.db.Exec(
		`INSERT OR REPLACE INTO messages
		(id, chat_jid, sender, content, timestamp, is_from_me, media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length, message_proto)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		id, chatJID, sender, content, timestamp, isFromMe, mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, messageProto,
	)
	return err
}

// GetMessageProto retrieves the serialized WebMessageInfo proto for a message
func (store *MessageStore) GetMessageProto(id, chatJID string) ([]byte, error) {
	var proto []byte
	err := store.db.QueryRow(
		"SELECT message_proto FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&proto)
	return proto, err
}

// Get messages from a chat
func (store *MessageStore) GetMessages(chatJID string, limit int) ([]Message, error) {
	rows, err := store.db.Query(
		"SELECT sender, content, timestamp, is_from_me, media_type, filename FROM messages WHERE chat_jid = ? ORDER BY timestamp DESC LIMIT ?",
		chatJID, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []Message
	for rows.Next() {
		var msg Message
		var timestamp time.Time
		err := rows.Scan(&msg.Sender, &msg.Content, &timestamp, &msg.IsFromMe, &msg.MediaType, &msg.Filename)
		if err != nil {
			return nil, err
		}
		msg.Time = timestamp
		messages = append(messages, msg)
	}

	return messages, nil
}

// Get all chats
func (store *MessageStore) GetChats() (map[string]time.Time, error) {
	rows, err := store.db.Query("SELECT jid, last_message_time FROM chats ORDER BY last_message_time DESC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	chats := make(map[string]time.Time)
	for rows.Next() {
		var jid string
		var lastMessageTime time.Time
		err := rows.Scan(&jid, &lastMessageTime)
		if err != nil {
			return nil, err
		}
		chats[jid] = lastMessageTime
	}

	return chats, nil
}

// Extract text content from a message
func extractTextContent(msg *waProto.Message) string {
	if msg == nil {
		return ""
	}

	// Try to get text content
	if text := msg.GetConversation(); text != "" {
		return text
	} else if extendedText := msg.GetExtendedTextMessage(); extendedText != nil {
		return extendedText.GetText()
	}

	// For now, we're ignoring non-text messages
	return ""
}

// SendMessageResponse represents the response for the send message API
type SendMessageResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// SendMessageRequest represents the request body for the send message API
type SendMessageRequest struct {
	Recipient string `json:"recipient"`
	Message   string `json:"message"`
	MediaPath string `json:"media_path,omitempty"`
}

// Function to send a WhatsApp message
// persistChatJID picks the chat row incoming history already uses.
// Sending to a phone JID while the live thread is @lid used to make
// outgoing text invisible to list_messages on the LID chat.
func persistChatJID(client *whatsmeow.Client, messageStore *MessageStore, recipient types.JID) types.JID {
	chat := recipient.ToNonAD()
	if chat.Server != types.DefaultUserServer || client.Store.LIDs == nil {
		return chat
	}
	lid, err := client.Store.LIDs.GetLIDForPN(context.Background(), chat)
	if err != nil || lid.IsEmpty() {
		return chat
	}
	lid = lid.ToNonAD()
	var existing string
	if err := messageStore.db.QueryRow("SELECT jid FROM chats WHERE jid = ?", lid.String()).Scan(&existing); err == nil && existing != "" {
		return lid
	}
	return chat
}

func persistOutgoing(client *whatsmeow.Client, messageStore *MessageStore, recipient types.JID, msg *waProto.Message, resp whatsmeow.SendResponse) {
	if messageStore == nil || msg == nil {
		return
	}
	chat := persistChatJID(client, messageStore, recipient)
	content := extractTextContent(msg)
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg)
	if content == "" && mediaType == "" {
		return
	}
	ts := resp.Timestamp
	if ts.IsZero() {
		ts = time.Now()
	}
	id := string(resp.ID)
	if id == "" {
		id = fmt.Sprintf("local-%d", ts.UnixNano())
	}
	sender := ""
	if client.Store.ID != nil {
		sender = client.Store.ID.User
	}
	name := chat.User
	var existing string
	if err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chat.String()).Scan(&existing); err == nil && existing != "" {
		name = existing
	}
	if err := messageStore.StoreChat(chat.String(), name, ts); err != nil {
		fmt.Printf("persistOutgoing: store chat: %v\n", err)
	}
	var protoBytes []byte
	protoBytes, _ = proto.Marshal(msg)
	if err := messageStore.StoreMessage(
		id, chat.String(), sender, content, ts, true,
		mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, protoBytes,
	); err != nil {
		fmt.Printf("persistOutgoing: store message: %v\n", err)
	}
}

func sendWhatsAppMessage(client *whatsmeow.Client, messageStore *MessageStore, recipient string, message string, mediaPath string) (bool, string) {
	if !client.IsConnected() {
		return false, "Not connected to WhatsApp"
	}

	// Create JID for recipient
	var recipientJID types.JID
	var err error

	// Check if recipient is a JID
	isJID := strings.Contains(recipient, "@")

	if isJID {
		// Parse the JID string
		recipientJID, err = types.ParseJID(recipient)
		if err != nil {
			return false, fmt.Sprintf("Error parsing JID: %v", err)
		}
	} else {
		// Create JID from phone number
		recipientJID = types.JID{
			User:   recipient,
			Server: "s.whatsapp.net", // For personal chats
		}
	}

	msg := &waProto.Message{}

	// Check if we have media to send
	if mediaPath != "" {
		// Read media file
		mediaData, err := os.ReadFile(mediaPath)
		if err != nil {
			return false, fmt.Sprintf("Error reading media file: %v", err)
		}

		// Determine media type and mime type based on file extension
		fileExt := strings.ToLower(mediaPath[strings.LastIndex(mediaPath, ".")+1:])
		var mediaType whatsmeow.MediaType
		var mimeType string

		// Handle different media types
		switch fileExt {
		// Image types
		case "jpg", "jpeg":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/jpeg"
		case "png":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/png"
		case "gif":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/gif"
		case "webp":
			mediaType = whatsmeow.MediaImage
			mimeType = "image/webp"

		// Audio types
		case "ogg":
			mediaType = whatsmeow.MediaAudio
			mimeType = "audio/ogg; codecs=opus"

		// Video types
		case "mp4":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/mp4"
		case "avi":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/avi"
		case "mov":
			mediaType = whatsmeow.MediaVideo
			mimeType = "video/quicktime"

		// Document types (for any other file type)
		default:
			mediaType = whatsmeow.MediaDocument
			mimeType = "application/octet-stream"
		}

		// Upload media to WhatsApp servers
		resp, err := client.Upload(context.Background(), mediaData, mediaType)
		if err != nil {
			return false, fmt.Sprintf("Error uploading media: %v", err)
		}

		fmt.Println("Media uploaded", resp)

		// Create the appropriate message type based on media type
		switch mediaType {
		case whatsmeow.MediaImage:
			msg.ImageMessage = &waProto.ImageMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaAudio:
			// Handle ogg audio files
			var seconds uint32 = 30 // Default fallback
			var waveform []byte = nil

			// Try to analyze the ogg file
			if strings.Contains(mimeType, "ogg") {
				analyzedSeconds, analyzedWaveform, err := analyzeOggOpus(mediaData)
				if err == nil {
					seconds = analyzedSeconds
					waveform = analyzedWaveform
				} else {
					return false, fmt.Sprintf("Failed to analyze Ogg Opus file: %v", err)
				}
			} else {
				fmt.Printf("Not an Ogg Opus file: %s\n", mimeType)
			}

			msg.AudioMessage = &waProto.AudioMessage{
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
				Seconds:       proto.Uint32(seconds),
				PTT:           proto.Bool(true),
				Waveform:      waveform,
			}
		case whatsmeow.MediaVideo:
			msg.VideoMessage = &waProto.VideoMessage{
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		case whatsmeow.MediaDocument:
			msg.DocumentMessage = &waProto.DocumentMessage{
				Title:         proto.String(mediaPath[strings.LastIndex(mediaPath, "/")+1:]),
				Caption:       proto.String(message),
				Mimetype:      proto.String(mimeType),
				URL:           &resp.URL,
				DirectPath:    &resp.DirectPath,
				MediaKey:      resp.MediaKey,
				FileEncSHA256: resp.FileEncSHA256,
				FileSHA256:    resp.FileSHA256,
				FileLength:    &resp.FileLength,
			}
		}
	} else {
		msg.Conversation = proto.String(message)
	}

	// Send message
	resp, err := client.SendMessage(context.Background(), recipientJID, msg)

	if err != nil {
		return false, fmt.Sprintf("Error sending message: %v", err)
	}

	persistOutgoing(client, messageStore, recipientJID, msg, resp)
	return true, fmt.Sprintf("Message sent to %s", recipient)
}

// Extract media info from a message
func extractMediaInfo(msg *waProto.Message) (mediaType string, filename string, url string, mediaKey []byte, fileSHA256 []byte, fileEncSHA256 []byte, fileLength uint64) {
	if msg == nil {
		return "", "", "", nil, nil, nil, 0
	}

	// Check for image message
	if img := msg.GetImageMessage(); img != nil {
		return "image", "image_" + time.Now().Format("20060102_150405") + ".jpg",
			img.GetURL(), img.GetMediaKey(), img.GetFileSHA256(), img.GetFileEncSHA256(), img.GetFileLength()
	}

	// Check for video message
	if vid := msg.GetVideoMessage(); vid != nil {
		return "video", "video_" + time.Now().Format("20060102_150405") + ".mp4",
			vid.GetURL(), vid.GetMediaKey(), vid.GetFileSHA256(), vid.GetFileEncSHA256(), vid.GetFileLength()
	}

	// Check for audio message
	if aud := msg.GetAudioMessage(); aud != nil {
		return "audio", "audio_" + time.Now().Format("20060102_150405") + ".ogg",
			aud.GetURL(), aud.GetMediaKey(), aud.GetFileSHA256(), aud.GetFileEncSHA256(), aud.GetFileLength()
	}

	// Check for document message
	if doc := msg.GetDocumentMessage(); doc != nil {
		filename := doc.GetFileName()
		if filename == "" {
			filename = "document_" + time.Now().Format("20060102_150405")
		}
		return "document", filename,
			doc.GetURL(), doc.GetMediaKey(), doc.GetFileSHA256(), doc.GetFileEncSHA256(), doc.GetFileLength()
	}

	return "", "", "", nil, nil, nil, 0
}

// Handle regular incoming messages with media support
func handleMessage(client *whatsmeow.Client, messageStore *MessageStore, msg *events.Message, logger waLog.Logger) {
	// Save message to database
	chatJID := msg.Info.Chat.String()
	sender := msg.Info.Sender.User

	// Get appropriate chat name (pass nil for conversation since we don't have one for regular messages)
	name := GetChatName(client, messageStore, msg.Info.Chat, chatJID, nil, sender, logger)

	// Update chat in database with the message timestamp (keeps last message time updated)
	err := messageStore.StoreChat(chatJID, name, msg.Info.Timestamp)
	if err != nil {
		logger.Warnf("Failed to store chat: %v", err)
	}

	// Extract text content
	content := extractTextContent(msg.Message)

	// Extract media info
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength := extractMediaInfo(msg.Message)

	// Skip if there's no content and no media
	if content == "" && mediaType == "" {
		return
	}

	// Store message in database
	// Serialize the message for potential MediaRetry later
	var msgProtoBytes []byte
	if msg.Message != nil {
		msgProtoBytes, _ = proto.Marshal(msg.Message)
	}

	err = messageStore.StoreMessage(
		msg.Info.ID,
		chatJID,
		sender,
		content,
		msg.Info.Timestamp,
		msg.Info.IsFromMe,
		mediaType,
		filename,
		url,
		mediaKey,
		fileSHA256,
		fileEncSHA256,
		fileLength,
		msgProtoBytes,
	)

	if err != nil {
		logger.Warnf("Failed to store message: %v", err)
	} else {
		// Log message reception
		timestamp := msg.Info.Timestamp.Format("2006-01-02 15:04:05")
		direction := "←"
		if msg.Info.IsFromMe {
			direction = "→"
		}

		// Log based on message type
		if mediaType != "" {
			fmt.Printf("[%s] %s %s: [%s: %s] %s\n", timestamp, direction, sender, mediaType, filename, content)
		} else if content != "" {
			fmt.Printf("[%s] %s %s: %s\n", timestamp, direction, sender, content)
		}
	}
}

// DownloadMediaRequest represents the request body for the download media API
type DownloadMediaRequest struct {
	MessageID string `json:"message_id"`
	ChatJID   string `json:"chat_jid"`
}

// DownloadMediaResponse represents the response for the download media API
type DownloadMediaResponse struct {
	Success  bool   `json:"success"`
	Message  string `json:"message"`
	Filename string `json:"filename,omitempty"`
	Path     string `json:"path,omitempty"`
}

// Store additional media info in the database
func (store *MessageStore) StoreMediaInfo(id, chatJID, url string, mediaKey, fileSHA256, fileEncSHA256 []byte, fileLength uint64) error {
	_, err := store.db.Exec(
		"UPDATE messages SET url = ?, media_key = ?, file_sha256 = ?, file_enc_sha256 = ?, file_length = ? WHERE id = ? AND chat_jid = ?",
		url, mediaKey, fileSHA256, fileEncSHA256, fileLength, id, chatJID,
	)
	return err
}

// Get media info from the database
func (store *MessageStore) GetMediaInfo(id, chatJID string) (string, string, string, []byte, []byte, []byte, uint64, error) {
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64

	err := store.db.QueryRow(
		"SELECT media_type, filename, url, media_key, file_sha256, file_enc_sha256, file_length FROM messages WHERE id = ? AND chat_jid = ?",
		id, chatJID,
	).Scan(&mediaType, &filename, &url, &mediaKey, &fileSHA256, &fileEncSHA256, &fileLength)

	return mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err
}

// MediaDownloader implements the whatsmeow.DownloadableMessage interface
type MediaDownloader struct {
	URL           string
	DirectPath    string
	MediaKey      []byte
	FileLength    uint64
	FileSHA256    []byte
	FileEncSHA256 []byte
	MediaType     whatsmeow.MediaType
}

// GetDirectPath implements the DownloadableMessage interface
func (d *MediaDownloader) GetDirectPath() string {
	return d.DirectPath
}

// GetURL implements the DownloadableMessage interface
func (d *MediaDownloader) GetURL() string {
	return d.URL
}

// GetMediaKey implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaKey() []byte {
	return d.MediaKey
}

// GetFileLength implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileLength() uint64 {
	return d.FileLength
}

// GetFileSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileSHA256() []byte {
	return d.FileSHA256
}

// GetFileEncSHA256 implements the DownloadableMessage interface
func (d *MediaDownloader) GetFileEncSHA256() []byte {
	return d.FileEncSHA256
}

// GetMediaType implements the DownloadableMessage interface
func (d *MediaDownloader) GetMediaType() whatsmeow.MediaType {
	return d.MediaType
}

// Function to download media from a message
func downloadMedia(client *whatsmeow.Client, messageStore *MessageStore, messageID, chatJID string) (bool, string, string, string, error) {
	// Query the database for the message
	var mediaType, filename, url string
	var mediaKey, fileSHA256, fileEncSHA256 []byte
	var fileLength uint64
	var err error

	// First, check if we already have this file
	chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
	localPath := ""

	// Get media info from the database
	mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength, err = messageStore.GetMediaInfo(messageID, chatJID)

	if err != nil {
		// Try to get basic info if extended info isn't available
		err = messageStore.db.QueryRow(
			"SELECT media_type, filename FROM messages WHERE id = ? AND chat_jid = ?",
			messageID, chatJID,
		).Scan(&mediaType, &filename)

		if err != nil {
			return false, "", "", "", fmt.Errorf("failed to find message: %v", err)
		}
	}

	// Check if this is a media message
	if mediaType == "" {
		return false, "", "", "", fmt.Errorf("not a media message")
	}

	// Create directory for the chat if it doesn't exist
	if err := os.MkdirAll(chatDir, 0755); err != nil {
		return false, "", "", "", fmt.Errorf("failed to create chat directory: %v", err)
	}

	// Generate a local path for the file
	localPath = fmt.Sprintf("%s/%s", chatDir, filename)

	// Get absolute path
	absPath, err := filepath.Abs(localPath)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to get absolute path: %v", err)
	}

	// Check if file already exists
	if _, err := os.Stat(localPath); err == nil {
		// File exists, return it
		return true, mediaType, filename, absPath, nil
	}

	// If we don't have all the media info we need, we can't download
	if url == "" || len(mediaKey) == 0 || len(fileSHA256) == 0 || len(fileEncSHA256) == 0 || fileLength == 0 {
		return false, "", "", "", fmt.Errorf("incomplete media information for download")
	}

	fmt.Printf("Attempting to download media for message %s in chat %s...\n", messageID, chatJID)

	// Extract direct path from URL
	directPath := extractDirectPathFromURL(url)

	// Create a downloader that implements DownloadableMessage
	var waMediaType whatsmeow.MediaType
	switch mediaType {
	case "image":
		waMediaType = whatsmeow.MediaImage
	case "video":
		waMediaType = whatsmeow.MediaVideo
	case "audio":
		waMediaType = whatsmeow.MediaAudio
	case "document":
		waMediaType = whatsmeow.MediaDocument
	default:
		return false, "", "", "", fmt.Errorf("unsupported media type: %s", mediaType)
	}

	downloader := &MediaDownloader{
		URL:           url,
		DirectPath:    directPath,
		MediaKey:      mediaKey,
		FileLength:    fileLength,
		FileSHA256:    fileSHA256,
		FileEncSHA256: fileEncSHA256,
		MediaType:     waMediaType,
	}

	// Download the media using whatsmeow client
	mediaData, err := client.Download(context.Background(), downloader)
	if err != nil {
		return false, "", "", "", fmt.Errorf("failed to download media: %v", err)
	}

	// Save the downloaded media to file
	if err := os.WriteFile(localPath, mediaData, 0644); err != nil {
		return false, "", "", "", fmt.Errorf("failed to save media file: %v", err)
	}

	fmt.Printf("Successfully downloaded %s media to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
	return true, mediaType, filename, absPath, nil
}

// Extract direct path from a WhatsApp media URL
func extractDirectPathFromURL(url string) string {
	// The direct path is typically in the URL, we need to extract it
	// Example URL: https://mmg.whatsapp.net/v/t62.7118-24/13812002_698058036224062_3424455886509161511_n.enc?ccb=11-4&oh=...

	// Find the path part after the domain
	parts := strings.SplitN(url, ".net/", 2)
	if len(parts) < 2 {
		return url // Return original URL if parsing fails
	}

	pathPart := parts[1]

	// Remove query parameters
	pathPart = strings.SplitN(pathPart, "?", 2)[0]

	// Create proper direct path format
	return "/" + pathPart
}

var (
	currentQRCode string // raw QR string for debugging
	currentQRPNG  []byte // QR rendered as PNG for HTTP serving

	// MediaRetry cache: message_id → pending retry request
	// When a download fails with 403/404/410, we send a retry receipt to the phone.
	// The phone re-uploads the media, and we get an events.MediaRetry with a new direct path.
	mediaRetryCache     = make(map[string]*pendingMediaRetry)
	mediaRetryCacheLock sync.Mutex
)

// pendingMediaRetry stores context needed to complete a media retry
type pendingMediaRetry struct {
	MessageID  string
	ChatJID    string
	MediaKey   []byte
	MediaType  string
	Filename   string
	FileLength uint64
	Result     chan *retryResult
}

type retryResult struct {
	DirectPath string
	Error      error
}

// handleMediaRetryEvent processes the response from the phone after
// SendMediaRetryReceipt was sent. The phone re-uploads the media and
// returns a new direct path.
//
// Per whatsmeow docs (mediaretry.go): the cached message object's
// DirectPath is updated, then cli.Download(msg) is called — which
// verifies file integrity via SHA256.
func handleMediaRetryEvent(evt *events.MediaRetry, logger waLog.Logger) {
	if evt.Error != nil {
		fmt.Printf("MediaRetry event received: id=%s, chat=%s, fromMe=%v, ERROR code=%d\n",
			evt.MessageID, evt.ChatID.String(), evt.FromMe, evt.Error.Code)
	} else {
		fmt.Printf("MediaRetry event received: id=%s, chat=%s, fromMe=%v, sender=%s, ciphertext=%d bytes\n",
			evt.MessageID, evt.ChatID.String(), evt.FromMe, evt.SenderID.String(), len(evt.Ciphertext))
	}
	mediaRetryCacheLock.Lock()
	pending, ok := mediaRetryCache[evt.MessageID]
	mediaRetryCacheLock.Unlock()

	if !ok {
		return
	}

	logger.Infof("MediaRetry response for %s", evt.MessageID)

	if evt.Error != nil {
		err := fmt.Errorf("media retry error (code %d)", evt.Error.Code)
		pending.Result <- &retryResult{Error: err}
		return
	}

	// Decrypt the retry notification using the media key
	retryData, err := whatsmeow.DecryptMediaRetryNotification(evt, pending.MediaKey)
	if err != nil {
		pending.Result <- &retryResult{Error: fmt.Errorf("failed to decrypt retry: %v", err)}
		return
	}

	if retryData.GetResult() != waMmsRetry.MediaRetryNotification_SUCCESS {
		pending.Result <- &retryResult{Error: fmt.Errorf("retry not successful")}
		return
	}

	// Success — we have a new direct path
	logger.Infof("MediaRetry success for %s: new path obtained", evt.MessageID)
	pending.Result <- &retryResult{DirectPath: retryData.GetDirectPath()}
}

// downloadWithRetry attempts to download media, and if it fails with
// 403/404/410, uses the canonical whatsmeow MediaRetry flow:
//
//  1. Deserialize the stored WebMessageInfo proto from DB
//  2. ParseWebMessage → get correct MessageInfo (with proper sender/participant)
//  3. SendMediaRetryReceipt with the correct MessageInfo + mediaKey
//  4. On events.MediaRetry response → update DirectPath in the message
//  5. cli.Download(msg) — full integrity verification via SHA256
//
// This follows the whatsmeow documentation in mediaretry.go exactly.
func downloadWithRetry(client *whatsmeow.Client, messageStore *MessageStore,
	messageID, chatJID string, maxRetries int) (bool, string, string, string, error) {

	for attempt := 0; attempt <= maxRetries; attempt++ {
		// Try to download normally first
		success, mediaType, filename, absPath, err := downloadMedia(client, messageStore, messageID, chatJID)
		if success {
			return true, mediaType, filename, absPath, nil
		}
		if err == nil {
			err = fmt.Errorf("download returned false with no error")
		}

		// Check if this is a retryable error (403/404/410)
		errStr := err.Error()
		isRetryable := strings.Contains(errStr, "403") || strings.Contains(errStr, "404") ||
			strings.Contains(errStr, "410") || strings.Contains(errStr, "status code")

		if !isRetryable || attempt >= maxRetries {
			return false, mediaType, filename, "", err
		}

		fmt.Printf("Download failed (%v), attempting media retry %d/%d for %s\n",
			err, attempt+1, maxRetries, messageID)

		// === Canonical whatsmeow MediaRetry flow ===

		// Step 1: Retrieve the serialized WebMessageInfo proto from DB
		protoBytes, protoErr := messageStore.GetMessageProto(messageID, chatJID)
		if protoErr != nil || len(protoBytes) == 0 {
			return false, mediaType, filename, "", fmt.Errorf("cannot retry: no stored proto (%v) (original: %v)", protoErr, err)
		}

		// Step 2: Deserialize into waWeb.WebMessageInfo
		var webMsg waWeb.WebMessageInfo
		if unmarshalErr := proto.Unmarshal(protoBytes, &webMsg); unmarshalErr != nil {
			return false, mediaType, filename, "", fmt.Errorf("failed to unmarshal proto: %v (original: %v)", unmarshalErr, err)
		}

		// Step 3: ParseWebMessage to get correct MessageInfo with proper sender
		jid, jidErr := types.ParseJID(chatJID)
		if jidErr != nil {
			return false, mediaType, filename, "", fmt.Errorf("cannot parse JID: %v", jidErr)
		}

		parsedEvt, parseErr := client.ParseWebMessage(jid, &webMsg)
		if parseErr != nil {
			return false, mediaType, filename, "", fmt.Errorf("ParseWebMessage failed: %v (original: %v)", parseErr, err)
		}

		// Step 4: Extract the media message part and its mediaKey
		// Per docs: "replace this with the part of the message you want to download"
		var downloadableMsg whatsmeow.DownloadableMessage
		var mediaKey []byte

		if img := parsedEvt.Message.GetImageMessage(); img != nil {
			downloadableMsg = img
			mediaKey = img.GetMediaKey()
		} else if doc := parsedEvt.Message.GetDocumentMessage(); doc != nil {
			downloadableMsg = doc
			mediaKey = doc.GetMediaKey()
		} else if vid := parsedEvt.Message.GetVideoMessage(); vid != nil {
			downloadableMsg = vid
			mediaKey = vid.GetMediaKey()
		} else if aud := parsedEvt.Message.GetAudioMessage(); aud != nil {
			downloadableMsg = aud
			mediaKey = aud.GetMediaKey()
		} else if stk := parsedEvt.Message.GetStickerMessage(); stk != nil {
			downloadableMsg = stk
			mediaKey = stk.GetMediaKey()
		}

		if downloadableMsg == nil || len(mediaKey) == 0 {
			return false, mediaType, filename, "", fmt.Errorf("no downloadable media found in message proto")
		}

		// Step 5: Set up pending retry in cache
		pending := &pendingMediaRetry{
			MessageID: messageID,
			ChatJID:   chatJID,
			MediaKey:  mediaKey,
			Result:    make(chan *retryResult, 1),
		}
		mediaRetryCacheLock.Lock()
		mediaRetryCache[messageID] = pending
		mediaRetryCacheLock.Unlock()

		// Step 6: Send MediaRetryReceipt using whatsmeow's public API.
		// This is the canonical method — the phone responds reliably.
		retryInfo := parsedEvt.Info

		fmt.Printf("Sending MediaRetry: chat=%s, sender=%s, id=%s, isFromMe=%v\n",
			retryInfo.Chat.String(), retryInfo.Sender.String(),
			retryInfo.ID, retryInfo.IsFromMe)

		// Get messageSecret for potential later use
		msgSecret, _, _ := client.Store.MsgSecrets.GetMessageSecret(
			context.Background(), parsedEvt.Info.Chat, parsedEvt.Info.Sender, types.MessageID(messageID))
		_ = msgSecret
		ownID := client.Store.ID.ToNonAD()

		retryErr := client.SendMediaRetryReceipt(context.Background(), &retryInfo, mediaKey)
		if retryErr != nil {
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, messageID)
			mediaRetryCacheLock.Unlock()
			return false, mediaType, filename, "", fmt.Errorf("SendMediaRetryReceipt failed: %v (original: %v)", retryErr, err)
		}

		fmt.Printf("MediaRetry receipt sent for %s, waiting for phone response...\n", messageID)

		// Step 7: Wait for the MediaRetry event
		select {
		case result := <-pending.Result:
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, messageID)
			mediaRetryCacheLock.Unlock()

			if result.Error != nil {
				errStr := result.Error.Error()
				if strings.Contains(errStr, "code 2") && attempt < maxRetries {
					// Error code 2: try PN participant, then on-demand history sync
					pnSender, pnErr := resolveLIDToPN(client, parsedEvt.Info.Sender)
					ownLID := client.Store.GetLID()

					if pnErr == nil && !pnSender.IsEmpty() && pnSender.String() != parsedEvt.Info.Sender.String() {
						for _, toAddr := range []types.JID{ownLID, ownID} {
							pnResultCh := make(chan *retryResult, 1)
							mediaRetryCacheLock.Lock()
							mediaRetryCache[messageID] = &pendingMediaRetry{Result: pnResultCh, MediaKey: mediaKey}
							mediaRetryCacheLock.Unlock()

							pnStanza := buildRetryStanza(toAddr, messageID, parsedEvt.Info.Chat, parsedEvt.Info.IsFromMe, pnSender, mediaKey)
							fmt.Printf("Sending MediaRetry (to=%s, participant=%s)\n", toAddr.String(), pnSender.String())
							if sendErr := client.DangerousInternals().SendNode(context.Background(), pnStanza); sendErr != nil {
								fmt.Printf("Send failed: %v\n", sendErr)
								continue
							}

							select {
							case pnResult := <-pnResultCh:
								mediaRetryCacheLock.Lock()
								delete(mediaRetryCache, messageID)
								mediaRetryCacheLock.Unlock()
								if pnResult.Error == nil {
									fmt.Printf("MediaRetry SUCCESS!\n")
									result = pnResult
									// Break out of error handling — fall through to download
									goto downloadMedia
								}
								fmt.Printf("Got error: %v\n", pnResult.Error)
							case <-time.After(15 * time.Second):
								fmt.Printf("Timeout\n")
								mediaRetryCacheLock.Lock()
								delete(mediaRetryCache, messageID)
								mediaRetryCacheLock.Unlock()
							}
						}
					}

					// On-demand history sync with newer anchor to get fresh URLs
					fmt.Printf("Requesting on-demand history sync with newer anchor...\n")
					_, odErr := requestOnDemandHistory(client, messageStore, messageID, chatJID)
					if odErr != nil {
						fmt.Printf("On-demand history sync failed: %v\n", odErr)
					} else {
						fmt.Printf("On-demand history sync done, retrying...\n")
						continue // retry loop — will re-read updated proto from DB
					}
				}
				return false, mediaType, filename, "", result.Error
			}

		downloadMedia:
			// Step 8: Update DirectPath in the message and download
			fmt.Printf("Got new media path for %s, downloading with SHA verification...\n", messageID)

			// Update the direct path on the downloadable message
			switch m := downloadableMsg.(type) {
			case *waProto.ImageMessage:
				m.DirectPath = &result.DirectPath
			case *waProto.DocumentMessage:
				m.DirectPath = &result.DirectPath
			case *waProto.VideoMessage:
				m.DirectPath = &result.DirectPath
			case *waProto.AudioMessage:
				m.DirectPath = &result.DirectPath
			case *waProto.StickerMessage:
				m.DirectPath = &result.DirectPath
			}

			mediaData, dlErr := client.Download(context.Background(), downloadableMsg)
			if dlErr != nil {
				return false, mediaType, filename, "", fmt.Errorf("download after retry failed: %v", dlErr)
			}

			// Save the downloaded media
			chatDir := fmt.Sprintf("store/%s", strings.ReplaceAll(chatJID, ":", "_"))
			if mkErr := os.MkdirAll(chatDir, 0755); mkErr != nil {
				return false, mediaType, filename, "", fmt.Errorf("failed to create dir: %v", mkErr)
			}
			if filename == "" {
				mType, dbFilename, _, _, _, _, _, _ := messageStore.GetMediaInfo(messageID, chatJID)
				if mType != "" {
					mediaType = mType
				}
				if dbFilename != "" {
					filename = dbFilename
				} else {
					filename = fmt.Sprintf("%s.%s", messageID, mediaType)
				}
			}
			localPath := fmt.Sprintf("%s/%s", chatDir, filename)
			absPath, _ := filepath.Abs(localPath)
			if writeErr := os.WriteFile(localPath, mediaData, 0644); writeErr != nil {
				return false, mediaType, filename, "", fmt.Errorf("failed to save: %v", writeErr)
			}

			fmt.Printf("Successfully downloaded (via retry) %s to %s (%d bytes)\n", mediaType, absPath, len(mediaData))
			return true, mediaType, filename, absPath, nil

		case <-time.After(60 * time.Second):
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, messageID)
			mediaRetryCacheLock.Unlock()
			return false, mediaType, filename, "", fmt.Errorf("media retry timeout (phone did not respond in 60s — make sure WhatsApp is open on the phone)")
		}
	}

	return false, "", "", "", fmt.Errorf("max retries exceeded")
}

// onDemandHistoryResult stores the result of an on-demand history sync
type onDemandHistoryResult struct {
	ProtoBytes []byte
	Error      error
}

// unavailableMessageResult stores the result of a BuildUnavailableMessageRequest
type unavailableMessageResult struct {
	ProtoBytes []byte
	Error      error
}

// unavailableMessageCache tracks pending unavailable message requests
var (
	unavailableMessageCache     = make(map[string]chan *unavailableMessageResult)
	unavailableMessageCacheLock sync.Mutex
)

// requestUnavailableMessage asks the phone to send a fresh copy of a message
// using BuildUnavailableMessageRequest. Per whatsmeow docs (send.go):
// "builds a message to request the user's primary device to send the copy of
// a message that this client was unable to decrypt."
//
// Unlike MediaRetry (which only checks phone media cache), this asks the phone
// to resend the ENTIRE message with fresh media credentials (new DirectPath,
// new SHA256). This works for messages where MediaRetry returns error code 2.
//
// The response comes as a ProtocolMessage with type PEER_DATA_OPERATION_REQUEST_RESPONSE_MESSAGE
// and is dispatched as *events.Message with UnavailableRequestID set.
func requestUnavailableMessage(client *whatsmeow.Client, messageStore *MessageStore,
	messageID, chatJID string) ([]byte, error) {

	// Set up response channel
	resultChan := make(chan *unavailableMessageResult, 1)
	unavailableMessageCacheLock.Lock()
	unavailableMessageCache[messageID] = resultChan
	unavailableMessageCacheLock.Unlock()

	defer func() {
		unavailableMessageCacheLock.Lock()
		delete(unavailableMessageCache, messageID)
		unavailableMessageCacheLock.Unlock()
	}()

	// Parse JID and get sender info
	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("cannot parse chat JID: %v", err)
	}

	// Get sender from DB
	var dbSender string
	var dbIsFromMe bool
	_ = messageStore.db.QueryRow(
		"SELECT sender, is_from_me FROM messages WHERE id = ? AND chat_jid = ?",
		messageID, chatJID,
	).Scan(&dbSender, &dbIsFromMe)

	// Try to get the real sender JID from the proto
	protoBytes, _ := messageStore.GetMessageProto(messageID, chatJID)
	var senderJID types.JID
	if len(protoBytes) > 0 {
		var webMsg waWeb.WebMessageInfo
		if unmarshalErr := proto.Unmarshal(protoBytes, &webMsg); unmarshalErr == nil {
			parsedEvt, parseErr := client.ParseWebMessage(jid, &webMsg)
			if parseErr == nil {
				senderJID = parsedEvt.Info.Sender
				dbIsFromMe = parsedEvt.Info.IsFromMe
			}
		}
	}
	if senderJID.IsEmpty() {
		// Fallback: parse sender from DB
		if dbSender != "" && len(dbSender) <= 15 {
			senderJID, _ = types.ParseJID(dbSender + "@s.whatsapp.net")
		}
		if senderJID.IsEmpty() {
			senderJID = jid // Use chat JID as fallback
		}
	}

	// Build the unavailable message request
	reqMsg := client.BuildUnavailableMessageRequest(jid, senderJID, messageID)
	if reqMsg == nil {
		return nil, fmt.Errorf("failed to build unavailable message request")
	}

	// Send via PeerMessage (to ourselves, processed by primary device)
	_, err = client.SendPeerMessage(context.Background(), reqMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to send unavailable message request: %v", err)
	}

	fmt.Printf("Sent unavailable message request for %s (chat=%s, sender=%s, fromMe=%v)\n",
		messageID, chatJID, senderJID.String(), dbIsFromMe)

	// Wait for the response
	select {
	case result := <-resultChan:
		if result.Error != nil {
			return nil, result.Error
		}
		return result.ProtoBytes, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("unavailable message request timeout")
	}
}

// onDemandHistoryCache tracks pending on-demand history sync requests
var (
	onDemandHistoryCache     = make(map[string]chan *onDemandHistoryResult)
	onDemandHistoryCacheLock sync.Mutex
)

// requestOnDemandHistory requests fresh message data from the phone for a
// specific chat via BuildHistorySyncRequest. This is used when MediaRetry
// returns error code 2 (media not available) — the initial history sync
// may have delivered incomplete data (missing MediaData.LocalPath).
//
// Per whatsmeow docs (send.go): "The response will come as an *events.HistorySync
// with type ON_DEMAND. The response will contain `count` messages immediately
// before the given message."
func requestOnDemandHistory(client *whatsmeow.Client, messageStore *MessageStore,
	messageID, chatJID string) ([]byte, error) {

	// Set up response channel
	resultChan := make(chan *onDemandHistoryResult, 1)
	onDemandHistoryCacheLock.Lock()
	onDemandHistoryCache[chatJID] = resultChan
	onDemandHistoryCacheLock.Unlock()

	defer func() {
		onDemandHistoryCacheLock.Lock()
		delete(onDemandHistoryCache, chatJID)
		onDemandHistoryCacheLock.Unlock()
	}()

	// Build the on-demand request for messages around the target
	jid, err := types.ParseJID(chatJID)
	if err != nil {
		return nil, fmt.Errorf("cannot parse JID: %v", err)
	}

	// Get timestamp of the target message
	var msgTimestamp time.Time
	var msgIsFromMe bool
	_ = messageStore.db.QueryRow(
		"SELECT timestamp, is_from_me FROM messages WHERE id = ? AND chat_jid = ?",
		messageID, chatJID,
	).Scan(&msgTimestamp, &msgIsFromMe)

	// IMPORTANT: BuildHistorySyncRequest returns messages BEFORE OldestMsgID.
	// To get the target message included, we must pass a NEWER message as anchor.
	// Find the next message after our target in the same chat.
	var newerMsgID string
	var newerMsgTimestamp time.Time
	var newerMsgIsFromMe bool
	_ = messageStore.db.QueryRow(
		"SELECT id, timestamp, is_from_me FROM messages WHERE chat_jid = ? AND timestamp > ? ORDER BY timestamp ASC LIMIT 1",
		chatJID, msgTimestamp,
	).Scan(&newerMsgID, &newerMsgTimestamp, &newerMsgIsFromMe)

	anchorID := messageID
	anchorTs := msgTimestamp
	anchorFromMe := msgIsFromMe
	if newerMsgID != "" {
		// Use the newer message as anchor so the target is included in results
		anchorID = newerMsgID
		anchorTs = newerMsgTimestamp
		anchorFromMe = newerMsgIsFromMe
		fmt.Printf("Using newer message %s as anchor (ts=%v) to include target %s\n", anchorID, anchorTs, messageID)
	}

	msgInfo := &types.MessageInfo{
		MessageSource: types.MessageSource{
			Chat:     jid,
			IsFromMe: anchorFromMe,
			IsGroup:  strings.Contains(chatJID, "@g.us"),
		},
		ID:        anchorID,
		Timestamp: anchorTs,
	}

	// Build and send on-demand history sync request
	historyMsg := client.BuildHistorySyncRequest(msgInfo, 50)
	if historyMsg == nil {
		return nil, fmt.Errorf("failed to build history sync request")
	}

	_, err = client.SendPeerMessage(context.Background(), historyMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to send on-demand history request: %v", err)
	}

	fmt.Printf("Sent on-demand history sync for chat %s (around msg %s)\n", chatJID, messageID)

	// Wait for the response
	select {
	case result := <-resultChan:
		if result.Error != nil {
			return nil, result.Error
		}
		return result.ProtoBytes, nil
	case <-time.After(30 * time.Second):
		return nil, fmt.Errorf("on-demand history sync timeout")
	}
}

// resolveLIDToPN resolves a LID JID to its phone number JID using whatsmeow's LID store.
func resolveLIDToPN(client *whatsmeow.Client, lid types.JID) (types.JID, error) {
	if lid.Server != types.HiddenUserServer {
		return lid, nil // already PN
	}
	pn, err := client.Store.LIDs.GetPNForLID(context.Background(), lid)
	if err != nil {
		return types.JID{}, err
	}
	return pn, nil
}

// buildRetryStanza builds a raw MediaRetry receipt stanza with proper JID types.
func buildRetryStanza(ownID types.JID, messageID string, chatJID types.JID, isFromMe bool, participant types.JID, mediaKey []byte) waBinary.Node {
	encCiphertext, encIV, _ := encryptMediaRetryReceiptLocal(messageID, mediaKey)

	content := []waBinary.Node{
		{Tag: "rmr", Attrs: waBinary.Attrs{
			"jid":         chatJID,
			"from_me":     isFromMe,
			"participant": participant,
		}},
	}
	if encCiphertext != nil {
		encryptNode := waBinary.Node{
			Tag: "encrypt",
			Content: []waBinary.Node{
				{Tag: "enc_p", Content: encCiphertext},
				{Tag: "enc_iv", Content: encIV},
			},
		}
		content = []waBinary.Node{encryptNode, content[0]}
	}

	return waBinary.Node{
		Tag: "receipt",
		Attrs: waBinary.Attrs{
			"id":   messageID,
			"to":   ownID,
			"type": "server-error",
		},
		Content: content,
	}
}

// encryptMediaRetryReceiptLocal encrypts a server error receipt using a key.
// Same algorithm as whatsmeow's private encryptMediaRetryReceipt.
func encryptMediaRetryReceiptLocal(messageID string, key []byte) (ciphertext, iv []byte, err error) {
	receipt := &waMmsRetry.ServerErrorReceipt{
		StanzaID: proto.String(messageID),
	}
	var plaintext []byte
	plaintext, err = proto.Marshal(receipt)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal payload: %w", err)
	}
	iv = random.Bytes(12)
	retryKey := hkdfutil.SHA256(key, nil, []byte("WhatsApp Media Retry Notification"), 32)
	ciphertext, err = gcmutil.Encrypt(retryKey, iv, plaintext, []byte(messageID))
	return
}

// Start a REST API server to expose the WhatsApp client functionality
func startRESTServer(client *whatsmeow.Client, messageStore *MessageStore, port int) {
	// Handler for QR pairing page (auto-refresh)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/qr" || r.URL.Path == "/login/status" ||
			r.URL.Path == "/api/send" || r.URL.Path == "/api/download" || r.URL.Path == "/api/resend" || r.URL.Path == "/api/mediaconn" || r.URL.Path == "/api/mediaretry" || r.URL.Path == "/api/histsyncerror" || r.URL.Path == "/api/mediaretry-lid" {
			// Let other handlers deal with these paths
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprintf(w, `<!DOCTYPE html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>WhatsApp QR</title><style>body{background:#111;color:#eee;display:flex;
flex-direction:column;align-items:center;justify-content:center;min-height:100vh;
margin:0;font-family:system-ui,sans-serif}img{max-width:400px;width:90vw;border-radius:12px}
#s{margin-top:16px;font-size:1.1em}</style></head><body>
<img id="q" src="/qr" alt="QR"><div id="s">...</div>
<script>async function r(){try{const d=await(await fetch('/login/status')).json();
document.getElementById('s').textContent='Status: '+d.status;if(d.status==='connected')
{document.getElementById('s').textContent='✅ Connected!';return;}}catch(e){}
document.getElementById('q').src='/qr?t='+Date.now()}setInterval(r,5000)</script>
</body></html>`)
	})

	// Handler for serving the current QR code as PNG image
	http.HandleFunc("/qr", func(w http.ResponseWriter, r *http.Request) {
		if currentQRCode == "" {
			http.Error(w, "No QR code available", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Write(currentQRPNG)
	})

	// Handler for serving login status
	http.HandleFunc("/login/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		status := "disconnected"
		if client.IsConnected() && client.IsLoggedIn() {
			status = "connected"
		} else if client.IsConnected() {
			status = "connecting"
		}
		json.NewEncoder(w).Encode(map[string]string{"status": status})
	})

	// Handler for sending messages
	http.HandleFunc("/api/send", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req SendMessageRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.Recipient == "" {
			http.Error(w, "Recipient is required", http.StatusBadRequest)
			return
		}

		if req.Message == "" && req.MediaPath == "" {
			http.Error(w, "Message or media path is required", http.StatusBadRequest)
			return
		}

		fmt.Println("Received request to send message", req.Message, req.MediaPath)

		// Send the message
		success, message := sendWhatsAppMessage(client, messageStore, req.Recipient, req.Message, req.MediaPath)
		fmt.Println("Message sent", success, message)
		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Set appropriate status code
		if !success {
			w.WriteHeader(http.StatusInternalServerError)
		}

		// Send response
		json.NewEncoder(w).Encode(SendMessageResponse{
			Success: success,
			Message: message,
		})
	})

	// Handler for downloading media
	// Handler for MediaRetry using LID instead of PN (matching WhatsApp Web behavior)
	http.HandleFunc("/api/mediaretry-lid", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ChatJID   string `json:"chat_jid"`
			MessageID string `json:"message_id"`
			Sender    string `json:"sender"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}

		chatJID, _ := types.ParseJID(req.ChatJID)
		senderJID, _ := types.ParseJID(req.Sender)

		// Get mediaKey
		_, _, _, mediaKey, _, _, _, pErr := messageStore.GetMediaInfo(req.MessageID, req.ChatJID)
		if pErr != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("failed to get media key: %v", pErr)})
			return
		}

		// Get own LID (not PN!) - this is what WhatsApp Web does
		ownLID := client.DangerousInternals().GetOwnLID().ToNonAD()
		if ownLID.IsEmpty() {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "no LID available"})
			return
		}
		fmt.Printf("MediaRetry-LID: ownLID=%s (instead of PN), chat=%s, sender=%s, id=%s\n",
			ownLID.String(), req.ChatJID, senderJID.String(), req.MessageID)

		// Encrypt the receipt
		msgInfo := &types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chatJID,
				Sender:   senderJID,
				IsFromMe: false,
				IsGroup:  strings.Contains(req.ChatJID, "@g.us"),
			},
			ID: req.MessageID,
		}

		// Set up media retry cache
		resultChan := make(chan *retryResult, 1)
		mediaRetryCacheLock.Lock()
		mediaRetryCache[req.MessageID] = &pendingMediaRetry{
			MessageID: req.MessageID,
			ChatJID:   req.ChatJID,
			MediaKey:  mediaKey,
			Result:    resultChan,
		}
		mediaRetryCacheLock.Unlock()

		// Build the receipt MANUALLY with to=LID
		// Replicate whatsmeow's SendMediaRetryReceipt but with ownLID
		ciphertext, iv, encErr := encryptMediaRetryReceiptLocal(req.MessageID, mediaKey)
		if encErr != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("encrypt failed: %v", encErr)})
			return
		}

		rmrAttrs := waBinary.Attrs{
			"jid":     chatJID,
			"from_me": false,
		}
		if msgInfo.IsGroup {
			rmrAttrs["participant"] = senderJID
		}

		sendErr := client.DangerousInternals().SendNode(context.Background(), waBinary.Node{
			Tag: "receipt",
			Attrs: waBinary.Attrs{
				"id":   req.MessageID,
				"to":   ownLID, // LID instead of PN!
				"type": "server-error",
			},
			Content: []waBinary.Node{
				{Tag: "encrypt", Content: []waBinary.Node{
					{Tag: "enc_p", Content: ciphertext},
					{Tag: "enc_iv", Content: iv},
				}},
				{Tag: "rmr", Attrs: rmrAttrs},
			},
		})

		if sendErr != nil {
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, req.MessageID)
			mediaRetryCacheLock.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("send failed: %v", sendErr)})
			return
		}

		// Wait for response
		select {
		case result := <-resultChan:
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, req.MessageID)
			mediaRetryCacheLock.Unlock()
			if result.Error != nil {
				json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("retry error: %v", result.Error)})
			} else {
				json.NewEncoder(w).Encode(map[string]any{"success": true, "direct_path": result.DirectPath})
			}
		case <-time.After(30 * time.Second):
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, req.MessageID)
			mediaRetryCacheLock.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "timeout"})
		}
	})

	// Handler for SendHistorySyncServerErrorReceipt
	http.HandleFunc("/api/histsyncerror", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ChatJID   string `json:"chat_jid"`
			MessageID string `json:"message_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}

		_, _, _, mediaKey, _, _, _, pErr := messageStore.GetMediaInfo(req.MessageID, req.ChatJID)
		if pErr != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("failed to get media key: %v", pErr)})
			return
		}

		err := client.SendHistorySyncServerErrorReceipt(context.Background(), types.MessageID(req.MessageID), mediaKey)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("send failed: %v", err)})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "message": "history sync server error receipt sent"})
	})

	// Handler for manual MediaRetry test
	http.HandleFunc("/api/mediaretry", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ChatJID   string `json:"chat_jid"`
			MessageID string `json:"message_id"`
			Sender    string `json:"sender"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}

		chatJID, _ := types.ParseJID(req.ChatJID)
		senderJID, _ := types.ParseJID(req.Sender)

		// Get mediaKey from message store
		mediaType, filename, url, mediaKey, fileSHA, fileEncSHA, fileLength, pErr := messageStore.GetMediaInfo(req.MessageID, req.ChatJID)
		if pErr != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("failed to get media info: %v", pErr)})
			return
		}
		_ = url
		_ = fileSHA
		_ = fileEncSHA
		_ = fileLength
		_ = mediaType
		_ = filename

		msgInfo := &types.MessageInfo{
			MessageSource: types.MessageSource{
				Chat:     chatJID,
				Sender:   senderJID,
				IsFromMe: false,
				IsGroup:  strings.Contains(req.ChatJID, "@g.us"),
			},
			ID: req.MessageID,
		}

		// Set up media retry cache
		resultChan := make(chan *retryResult, 1)
		mediaRetryCacheLock.Lock()
		mediaRetryCache[req.MessageID] = &pendingMediaRetry{
			MessageID: req.MessageID,
			ChatJID:   req.ChatJID,
			MediaKey:  mediaKey,
			Result:    resultChan,
		}
		mediaRetryCacheLock.Unlock()

		fmt.Printf("Manual MediaRetry: chat=%s, sender=%s, id=%s\n",
			req.ChatJID, senderJID.String(), req.MessageID)

		retryErr := client.SendMediaRetryReceipt(context.Background(), msgInfo, mediaKey)
		if retryErr != nil {
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, req.MessageID)
			mediaRetryCacheLock.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("send failed: %v", retryErr)})
			return
		}

		// Wait for response
		select {
		case result := <-resultChan:
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, req.MessageID)
			mediaRetryCacheLock.Unlock()
			if result.Error != nil {
				json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("retry error: %v", result.Error)})
			} else {
				json.NewEncoder(w).Encode(map[string]any{"success": true, "direct_path": result.DirectPath})
			}
		case <-time.After(30 * time.Second):
			mediaRetryCacheLock.Lock()
			delete(mediaRetryCache, req.MessageID)
			mediaRetryCacheLock.Unlock()
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "timeout"})
		}
	})

	// Handler for getting media connection info (for debugging)
	http.HandleFunc("/api/mediaconn", func(w http.ResponseWriter, r *http.Request) {
		mc, err := client.DangerousInternals().RefreshMediaConn(context.Background(), true)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}
		hosts := make([]string, len(mc.Hosts))
		for i, h := range mc.Hosts {
			hosts[i] = h.Hostname
		}
		json.NewEncoder(w).Encode(map[string]any{
			"auth":    mc.Auth,
			"authTTL": mc.AuthTTL,
			"ttl":     mc.TTL,
			"hosts":   hosts,
		})
	})

	// Handler for PlaceholderMessageResend (fresh message with fresh URLs)
	http.HandleFunc("/api/resend", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			ChatJID   string `json:"chat_jid"`
			MessageID string `json:"message_id"`
			SenderPN  string `json:"sender_pn"` // Optional: phone number JID
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": err.Error()})
			return
		}

		chatJID, _ := types.ParseJID(req.ChatJID)
		var senderJID types.JID
		if req.SenderPN != "" {
			senderJID, _ = types.ParseJID(req.SenderPN)
		} else {
			// Try to get sender from message store
			senderJID = chatJID
		}

		// Build PlaceholderMessageResend request with explicit participant
		msgKey := &waCommon.MessageKey{
			FromMe:    proto.Bool(false),
			ID:        proto.String(req.MessageID),
			RemoteJID: proto.String(req.ChatJID),
		}
		if !senderJID.IsEmpty() {
			msgKey.Participant = proto.String(senderJID.ToNonAD().String())
		}

		resendMsg := &waE2E.Message{
			ProtocolMessage: &waE2E.ProtocolMessage{
				Type: waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_MESSAGE.Enum(),
				PeerDataOperationRequestMessage: &waE2E.PeerDataOperationRequestMessage{
					PeerDataOperationRequestType: waE2E.PeerDataOperationRequestType_PLACEHOLDER_MESSAGE_RESEND.Enum(),
					PlaceholderMessageResendRequest: []*waE2E.PeerDataOperationRequestMessage_PlaceholderMessageResendRequest{{
						MessageKey: msgKey,
					}},
				},
			},
		}

		fmt.Printf("Sending PlaceholderMessageResend: chat=%s, id=%s, participant=%s\n",
			req.ChatJID, req.MessageID, senderJID.String())

		_, err := client.SendPeerMessage(context.Background(), resendMsg)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]any{"success": false, "message": fmt.Sprintf("send failed: %v", err)})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"success": true, "message": "resend request sent"})
	})

	http.HandleFunc("/api/download", func(w http.ResponseWriter, r *http.Request) {
		// Only allow POST requests
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Parse the request body
		var req DownloadMediaRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Invalid request format", http.StatusBadRequest)
			return
		}

		// Validate request
		if req.MessageID == "" || req.ChatJID == "" {
			http.Error(w, "Message ID and Chat JID are required", http.StatusBadRequest)
			return
		}

		// Download the media (with automatic media retry for expired URLs)
		success, mediaType, filename, path, err := downloadWithRetry(client, messageStore, req.MessageID, req.ChatJID, 2)

		// Set response headers
		w.Header().Set("Content-Type", "application/json")

		// Handle download result
		if !success || err != nil {
			errMsg := "Unknown error"
			if err != nil {
				errMsg = err.Error()
			}

			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(DownloadMediaResponse{
				Success: false,
				Message: fmt.Sprintf("Failed to download media: %s", errMsg),
			})
			return
		}

		// Send successful response
		json.NewEncoder(w).Encode(DownloadMediaResponse{
			Success:  true,
			Message:  fmt.Sprintf("Successfully downloaded %s media", mediaType),
			Filename: filename,
			Path:     path,
		})
	})

	// Start the server
	serverAddr := fmt.Sprintf(":%d", port)
	fmt.Printf("Starting REST API server on %s...\n", serverAddr)

	// Run server in a goroutine so it doesn't block
	go func() {
		if err := http.ListenAndServe(serverAddr, nil); err != nil {
			fmt.Printf("REST API server error: %v\n", err)
		}
	}()
}

func main() {
	// Set up logger
	logger := waLog.Stdout("Client", "DEBUG", true)
	logger.Infof("Starting WhatsApp client...")

	// Create database connection for storing session data
	dbLog := waLog.Stdout("Database", "INFO", true)

	// Create directory for database if it doesn't exist
	if err := os.MkdirAll("store", 0755); err != nil {
		logger.Errorf("Failed to create store directory: %v", err)
		return
	}

	container, err := sqlstore.New(context.Background(), "sqlite3", "file:store/whatsapp.db?_foreign_keys=on", dbLog)
	if err != nil {
		logger.Errorf("Failed to connect to database: %v", err)
		return
	}

	// Get device store - This contains session information
	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		if err == sql.ErrNoRows {
			// No device exists, create one
			deviceStore = container.NewDevice()
			logger.Infof("Created new device")
		} else {
			logger.Errorf("Failed to get device: %v", err)
			return
		}
	}

	// Set device identity to mimic Google Chrome on Windows
	// This affects how the device appears in WhatsApp → Linked Devices
	store.SetOSInfo("Windows", [3]uint32{128, 0, 0})
	// Override platform type from UNKNOWN to CHROME so WhatsApp shows
	// "Google Chrome (Windows)" instead of just "Windows"
	store.DeviceProps.PlatformType = waCompanionReg.DeviceProps_CHROME.Enum()

	// Create client instance
	client := whatsmeow.NewClient(deviceStore, logger)
	if client == nil {
		logger.Errorf("Failed to create WhatsApp client")
		return
	}

	// Initialize message store
	messageStore, err := NewMessageStore()
	if err != nil {
		logger.Errorf("Failed to initialize message store: %v", err)
		return
	}
	defer messageStore.Close()

	// Setup event handling for messages and history sync
	client.AddEventHandler(func(evt interface{}) {
		switch v := evt.(type) {
		case *events.Message:
			// Check if this is a response to an unavailable message request
			if v.UnavailableRequestID != "" {
				fmt.Printf("Received unavailable message response for %s\n", v.UnavailableRequestID)
				unavailableMessageCacheLock.Lock()
				resultChan, ok := unavailableMessageCache[v.UnavailableRequestID]
				unavailableMessageCacheLock.Unlock()

				if ok {
					// Serialize the fresh message proto
					var freshProto []byte
					if v.Message != nil {
						freshProto, _ = proto.Marshal(v.Message)
					}
					// Update the message proto in DB
					if len(freshProto) > 0 {
						_, _ = messageStore.db.Exec(
							"UPDATE messages SET message_proto = ? WHERE id = ? AND chat_jid = ?",
							freshProto, v.UnavailableRequestID, v.Info.Chat.String(),
						)
						// Also update media fields from the fresh message
						var mediaType, filename, url string
						var mediaKey, fileSHA256, fileEncSHA256 []byte
						var fileLength uint64
						if v.Message != nil {
							mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(v.Message)
						}
						if mediaType != "" {
							messageStore.StoreMediaInfo(v.UnavailableRequestID, v.Info.Chat.String(), url, mediaKey, fileSHA256, fileEncSHA256, fileLength)
						}
						_ = filename
						logger.Infof("Unavailable message: updated proto (%d bytes) and media for %s", len(freshProto), v.UnavailableRequestID)
					}
					select {
					case resultChan <- &unavailableMessageResult{ProtoBytes: freshProto}:
					default:
					}
					return
				}
			}

			// Process regular messages
			handleMessage(client, messageStore, v, logger)

		case *events.HistorySync:
			// Process history sync events
			handleHistorySync(client, messageStore, v, logger)

		case *events.Connected:
			logger.Infof("Connected to WhatsApp")

		case *events.LoggedOut:
			logger.Warnf("Device logged out, please scan QR code to log in again")

		case *events.MediaRetry:
			handleMediaRetryEvent(v, logger)
		}
	})

	// Start REST API server in background BEFORE QR pairing,
	// so /qr and /login/status endpoints are available while waiting for scan.
	go startRESTServer(client, messageStore, 8080)

	// Create channel to track connection success
	connected := make(chan bool, 1)

	// Connect to WhatsApp
	if client.Store.ID == nil {
		// No ID stored, this is a new client, need to pair with phone
		qrChan, _ := client.GetQRChannel(context.Background())
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}

		// Print QR code for pairing with phone
		for evt := range qrChan {
			if evt.Event == "code" {
				currentQRCode = evt.Code
				fmt.Println("\nScan this QR code with your WhatsApp app:")
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
				// Also render as PNG for HTTP endpoint
				if png, err := qr.Encode(evt.Code, qr.M); err == nil {
					currentQRPNG = png.PNG()
				}
			} else if evt.Event == "success" {
				currentQRCode = ""
				currentQRPNG = nil
				connected <- true
				break
			}
		}

		// Wait for connection
		select {
		case <-connected:
			fmt.Println("\nSuccessfully connected and authenticated!")
		case <-time.After(3 * time.Minute):
			logger.Errorf("Timeout waiting for QR code scan")
			return
		}
	} else {
		// Already logged in, just connect
		err = client.Connect()
		if err != nil {
			logger.Errorf("Failed to connect: %v", err)
			return
		}
		connected <- true
	}

	// Wait a moment for connection to stabilize
	time.Sleep(2 * time.Second)

	if !client.IsConnected() {
		logger.Errorf("Failed to establish stable connection")
		return
	}

	fmt.Println("\n✓ Connected to WhatsApp! Type 'help' for commands.")

	// Create a channel to keep the main goroutine alive
	exitChan := make(chan os.Signal, 1)
	signal.Notify(exitChan, syscall.SIGINT, syscall.SIGTERM)

	fmt.Println("REST server is running. Press Ctrl+C to disconnect and exit.")

	// Wait for termination signal
	<-exitChan

	fmt.Println("Disconnecting...")
	// Disconnect client
	client.Disconnect()
}

// GetChatName determines the appropriate name for a chat based on JID and other info
func GetChatName(client *whatsmeow.Client, messageStore *MessageStore, jid types.JID, chatJID string, conversation interface{}, sender string, logger waLog.Logger) string {
	// First, check if chat already exists in database with a name
	var existingName string
	err := messageStore.db.QueryRow("SELECT name FROM chats WHERE jid = ?", chatJID).Scan(&existingName)
	if err == nil && existingName != "" {
		// Chat exists with a name, use that
		logger.Infof("Using existing chat name for %s: %s", chatJID, existingName)
		return existingName
	}

	// Need to determine chat name
	var name string

	if jid.Server == "g.us" {
		// This is a group chat
		logger.Infof("Getting name for group: %s", chatJID)

		// Use conversation data if provided (from history sync)
		if conversation != nil {
			// Extract name from conversation if available
			// This uses type assertions to handle different possible types
			var displayName, convName *string
			// Try to extract the fields we care about regardless of the exact type
			v := reflect.ValueOf(conversation)
			if v.Kind() == reflect.Ptr && !v.IsNil() {
				v = v.Elem()

				// Try to find DisplayName field
				if displayNameField := v.FieldByName("DisplayName"); displayNameField.IsValid() && displayNameField.Kind() == reflect.Ptr && !displayNameField.IsNil() {
					dn := displayNameField.Elem().String()
					displayName = &dn
				}

				// Try to find Name field
				if nameField := v.FieldByName("Name"); nameField.IsValid() && nameField.Kind() == reflect.Ptr && !nameField.IsNil() {
					n := nameField.Elem().String()
					convName = &n
				}
			}

			// Use the name we found
			if displayName != nil && *displayName != "" {
				name = *displayName
			} else if convName != nil && *convName != "" {
				name = *convName
			}
		}

		// If we didn't get a name, try group info
		if name == "" {
			groupInfo, err := client.GetGroupInfo(context.Background(), jid)
			if err == nil && groupInfo.Name != "" {
				name = groupInfo.Name
			} else {
				// Fallback name for groups
				name = fmt.Sprintf("Group %s", jid.User)
			}
		}

		logger.Infof("Using group name: %s", name)
	} else {
		// This is an individual contact
		logger.Infof("Getting name for contact: %s", chatJID)

		// Just use contact info (full name)
		contact, err := client.Store.Contacts.GetContact(context.Background(), jid)
		if err == nil && contact.FullName != "" {
			name = contact.FullName
		} else if sender != "" {
			// Fallback to sender
			name = sender
		} else {
			// Last fallback to JID
			name = jid.User
		}

		logger.Infof("Using contact name: %s", name)
	}

	return name
}

// Handle history sync events
func handleHistorySync(client *whatsmeow.Client, messageStore *MessageStore, historySync *events.HistorySync, logger waLog.Logger) {
	// Check for ON_DEMAND history sync (response to requestOnDemandHistory)
	syncType := historySync.Data.GetSyncType()
	if syncType == waHistorySync.HistorySync_ON_DEMAND {
		fmt.Printf("Received ON_DEMAND history sync with %d conversations\n", len(historySync.Data.Conversations))

		// Find the chat we requested and extract updated proto data
		for _, conversation := range historySync.Data.Conversations {
			if conversation.ID == nil {
				continue
			}
			chatJID := *conversation.ID

			// Notify the waiting requestOnDemandHistory caller
			onDemandHistoryCacheLock.Lock()
			resultChan, ok := onDemandHistoryCache[chatJID]
			onDemandHistoryCacheLock.Unlock()

			if !ok {
				continue
			}

			// Extract the first message proto (should contain our target message)
			for _, msg := range conversation.Messages {
				if msg == nil || msg.Message == nil {
					continue
				}
				protoBytes, mErr := proto.Marshal(msg.Message)
				if mErr != nil {
					continue
				}

				// Get message ID
				var msgID string
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Update the proto in the DB
				_, _ = messageStore.db.Exec(
					"UPDATE messages SET message_proto = ? WHERE id = ? AND chat_jid = ?",
					protoBytes, msgID, chatJID,
				)

				// Also update media fields if present
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64
				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}
				_ = filename // already in proto
				if mediaType != "" {
					messageStore.StoreMediaInfo(msgID, chatJID, url, mediaKey, fileSHA256, fileEncSHA256, fileLength)
				}

				logger.Infof("ON_DEMAND: updated proto for %s in %s", msgID, chatJID)
			}

			// Send result to the waiting caller
			select {
			case resultChan <- &onDemandHistoryResult{Error: nil}:
			default:
			}
			return
		}

		// Chat not found in the response
		onDemandHistoryCacheLock.Lock()
		for chatJID, resultChan := range onDemandHistoryCache {
			select {
			case resultChan <- &onDemandHistoryResult{Error: fmt.Errorf("chat %s not found in on-demand response", chatJID)}:
			default:
			}
		}
		onDemandHistoryCacheLock.Unlock()
		return
	}

	fmt.Printf("Received history sync event with %d conversations\n", len(historySync.Data.Conversations))

	syncedCount := 0
	for _, conversation := range historySync.Data.Conversations {
		// Parse JID from the conversation
		if conversation.ID == nil {
			continue
		}

		chatJID := *conversation.ID

		// Try to parse the JID
		jid, err := types.ParseJID(chatJID)
		if err != nil {
			logger.Warnf("Failed to parse JID %s: %v", chatJID, err)
			continue
		}

		// Get appropriate chat name by passing the history sync conversation directly
		name := GetChatName(client, messageStore, jid, chatJID, conversation, "", logger)

		// Process messages
		messages := conversation.Messages
		if len(messages) > 0 {
			// Update chat with latest message timestamp
			latestMsg := messages[0]
			if latestMsg == nil || latestMsg.Message == nil {
				continue
			}

			// Get timestamp from message info
			timestamp := time.Time{}
			if ts := latestMsg.Message.GetMessageTimestamp(); ts != 0 {
				timestamp = time.Unix(int64(ts), 0)
			} else {
				continue
			}

			messageStore.StoreChat(chatJID, name, timestamp)

			// Store messages
			for _, msg := range messages {
				if msg == nil || msg.Message == nil {
					continue
				}

				// Extract text content
				var content string
				if msg.Message.Message != nil {
					if conv := msg.Message.Message.GetConversation(); conv != "" {
						content = conv
					} else if ext := msg.Message.Message.GetExtendedTextMessage(); ext != nil {
						content = ext.GetText()
					}
				}

				// Extract media info
				var mediaType, filename, url string
				var mediaKey, fileSHA256, fileEncSHA256 []byte
				var fileLength uint64

				if msg.Message.Message != nil {
					mediaType, filename, url, mediaKey, fileSHA256, fileEncSHA256, fileLength = extractMediaInfo(msg.Message.Message)
				}

				// Log the message content for debugging
				logger.Infof("Message content: %v, Media Type: %v", content, mediaType)

				// Skip messages with no content and no media
				if content == "" && mediaType == "" {
					continue
				}

				// Determine sender
				var sender string
				isFromMe := false
				if msg.Message.Key != nil {
					if msg.Message.Key.FromMe != nil {
						isFromMe = *msg.Message.Key.FromMe
					}
					if !isFromMe && msg.Message.Key.Participant != nil && *msg.Message.Key.Participant != "" {
						sender = *msg.Message.Key.Participant
					} else if isFromMe {
						sender = client.Store.ID.User
					} else {
						// For groups without participant, log warning for debugging
						if strings.Contains(chatJID, "@g.us") {
							logger.Warnf("Group message has no participant, using group JID as fallback")
						}
						sender = jid.User
					}
				} else {
					sender = jid.User
				}

				// Store message
				msgID := ""
				if msg.Message.Key != nil && msg.Message.Key.ID != nil {
					msgID = *msg.Message.Key.ID
				}

				// Get message timestamp
				timestamp := time.Time{}
				if ts := msg.Message.GetMessageTimestamp(); ts != 0 {
					timestamp = time.Unix(int64(ts), 0)
				} else {
					continue
				}

				// Serialize the full WebMessageInfo proto for potential MediaRetry
				var historyProtoBytes []byte
				if msg.Message != nil {
					historyProtoBytes, _ = proto.Marshal(msg.Message)
				}

				err = messageStore.StoreMessage(
					msgID,
					chatJID,
					sender,
					content,
					timestamp,
					isFromMe,
					mediaType,
					filename,
					url,
					mediaKey,
					fileSHA256,
					fileEncSHA256,
					fileLength,
					historyProtoBytes,
				)
				if err != nil {
					logger.Warnf("Failed to store history message: %v", err)
				} else {
					syncedCount++
					// Log successful message storage
					if mediaType != "" {
						logger.Infof("Stored message: [%s] %s -> %s: [%s: %s] %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, mediaType, filename, content)
					} else {
						logger.Infof("Stored message: [%s] %s -> %s: %s",
							timestamp.Format("2006-01-02 15:04:05"), sender, chatJID, content)
					}
				}
			}
		}
	}

	fmt.Printf("History sync complete. Stored %d messages.\n", syncedCount)
}

// Request history sync from the server
func requestHistorySync(client *whatsmeow.Client) {
	if client == nil {
		fmt.Println("Client is not initialized. Cannot request history sync.")
		return
	}

	if !client.IsConnected() {
		fmt.Println("Client is not connected. Please ensure you are connected to WhatsApp first.")
		return
	}

	if client.Store.ID == nil {
		fmt.Println("Client is not logged in. Please scan the QR code first.")
		return
	}

	// Build and send a history sync request
	historyMsg := client.BuildHistorySyncRequest(nil, 100)
	if historyMsg == nil {
		fmt.Println("Failed to build history sync request.")
		return
	}

	_, err := client.SendMessage(context.Background(), types.JID{
		Server: "s.whatsapp.net",
		User:   "status",
	}, historyMsg)

	if err != nil {
		fmt.Printf("Failed to request history sync: %v\n", err)
	} else {
		fmt.Println("History sync requested. Waiting for server response...")
	}
}

// analyzeOggOpus tries to extract duration and generate a simple waveform from an Ogg Opus file
func analyzeOggOpus(data []byte) (duration uint32, waveform []byte, err error) {
	// Try to detect if this is a valid Ogg file by checking for the "OggS" signature
	// at the beginning of the file
	if len(data) < 4 || string(data[0:4]) != "OggS" {
		return 0, nil, fmt.Errorf("not a valid Ogg file (missing OggS signature)")
	}

	// Parse Ogg pages to find the last page with a valid granule position
	var lastGranule uint64
	var sampleRate uint32 = 48000 // Default Opus sample rate
	var preSkip uint16 = 0
	var foundOpusHead bool

	// Scan through the file looking for Ogg pages
	for i := 0; i < len(data); {
		// Check if we have enough data to read Ogg page header
		if i+27 >= len(data) {
			break
		}

		// Verify Ogg page signature
		if string(data[i:i+4]) != "OggS" {
			// Skip until next potential page
			i++
			continue
		}

		// Extract header fields
		granulePos := binary.LittleEndian.Uint64(data[i+6 : i+14])
		pageSeqNum := binary.LittleEndian.Uint32(data[i+18 : i+22])
		numSegments := int(data[i+26])

		// Extract segment table
		if i+27+numSegments >= len(data) {
			break
		}
		segmentTable := data[i+27 : i+27+numSegments]

		// Calculate page size
		pageSize := 27 + numSegments
		for _, segLen := range segmentTable {
			pageSize += int(segLen)
		}

		// Check if we're looking at an OpusHead packet (should be in first few pages)
		if !foundOpusHead && pageSeqNum <= 1 {
			// Look for "OpusHead" marker in this page
			pageData := data[i : i+pageSize]
			headPos := bytes.Index(pageData, []byte("OpusHead"))
			if headPos >= 0 && headPos+12 < len(pageData) {
				// Found OpusHead, extract sample rate and pre-skip
				// OpusHead format: Magic(8) + Version(1) + Channels(1) + PreSkip(2) + SampleRate(4) + ...
				headPos += 8 // Skip "OpusHead" marker
				// PreSkip is 2 bytes at offset 10
				if headPos+12 <= len(pageData) {
					preSkip = binary.LittleEndian.Uint16(pageData[headPos+10 : headPos+12])
					sampleRate = binary.LittleEndian.Uint32(pageData[headPos+12 : headPos+16])
					foundOpusHead = true
					fmt.Printf("Found OpusHead: sampleRate=%d, preSkip=%d\n", sampleRate, preSkip)
				}
			}
		}

		// Keep track of last valid granule position
		if granulePos != 0 {
			lastGranule = granulePos
		}

		// Move to next page
		i += pageSize
	}

	if !foundOpusHead {
		fmt.Println("Warning: OpusHead not found, using default values")
	}

	// Calculate duration based on granule position
	if lastGranule > 0 {
		// Formula for duration: (lastGranule - preSkip) / sampleRate
		durationSeconds := float64(lastGranule-uint64(preSkip)) / float64(sampleRate)
		duration = uint32(math.Ceil(durationSeconds))
		fmt.Printf("Calculated Opus duration from granule: %f seconds (lastGranule=%d)\n",
			durationSeconds, lastGranule)
	} else {
		// Fallback to rough estimation if granule position not found
		fmt.Println("Warning: No valid granule position found, using estimation")
		durationEstimate := float64(len(data)) / 2000.0 // Very rough approximation
		duration = uint32(durationEstimate)
	}

	// Make sure we have a reasonable duration (at least 1 second, at most 300 seconds)
	if duration < 1 {
		duration = 1
	} else if duration > 300 {
		duration = 300
	}

	// Generate waveform
	waveform = placeholderWaveform(duration)

	fmt.Printf("Ogg Opus analysis: size=%d bytes, calculated duration=%d sec, waveform=%d bytes\n",
		len(data), duration, len(waveform))

	return duration, waveform, nil
}

// min returns the smaller of x or y
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

// placeholderWaveform generates a synthetic waveform for WhatsApp voice messages
// that appears natural with some variability based on the duration
func placeholderWaveform(duration uint32) []byte {
	// WhatsApp expects a 64-byte waveform for voice messages
	const waveformLength = 64
	waveform := make([]byte, waveformLength)

	// Seed the random number generator for consistent results with the same duration
	rand.Seed(int64(duration))

	// Create a more natural looking waveform with some patterns and variability
	// rather than completely random values

	// Base amplitude and frequency - longer messages get faster frequency
	baseAmplitude := 35.0
	frequencyFactor := float64(min(int(duration), 120)) / 30.0

	for i := range waveform {
		// Position in the waveform (normalized 0-1)
		pos := float64(i) / float64(waveformLength)

		// Create a wave pattern with some randomness
		// Use multiple sine waves of different frequencies for more natural look
		val := baseAmplitude * math.Sin(pos*math.Pi*frequencyFactor*8)
		val += (baseAmplitude / 2) * math.Sin(pos*math.Pi*frequencyFactor*16)

		// Add some randomness to make it look more natural
		val += (rand.Float64() - 0.5) * 15

		// Add some fade-in and fade-out effects
		fadeInOut := math.Sin(pos * math.Pi)
		val = val * (0.7 + 0.3*fadeInOut)

		// Center around 50 (typical voice baseline)
		val = val + 50

		// Ensure values stay within WhatsApp's expected range (0-100)
		if val < 0 {
			val = 0
		} else if val > 100 {
			val = 100
		}

		waveform[i] = byte(val)
	}

	return waveform
}
