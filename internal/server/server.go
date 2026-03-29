package server

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"mog/internal/config"
	"mog/internal/logging"
	mongo "mog/internal/mongo/handler"
	mwire "mog/internal/mongo/wire"
	"mog/internal/translator"
)

var (
	readerPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewReaderSize(nil, 1<<16) // 64KB is usually enough and less memory intensive than 1MB
		},
	}
	writerPool = sync.Pool{
		New: func() interface{} {
			return bufio.NewWriterSize(nil, 1<<16)
		},
	}
)

func getReader(rd io.Reader) *bufio.Reader {
	r := readerPool.Get().(*bufio.Reader)
	r.Reset(rd)
	return r
}

func putReader(r *bufio.Reader) {
	r.Reset(nil)
	readerPool.Put(r)
}

func getWriter(wr io.Writer) *bufio.Writer {
	w := writerPool.Get().(*bufio.Writer)
	w.Reset(wr)
	return w
}

func putWriter(w *bufio.Writer) {
	w.Reset(nil)
	writerPool.Put(w)
}

// Server is the main server struct.
type Server struct {
	listener   net.Listener
	pool       *pgxpool.Pool
	config     *config.Config
	translator *translator.Translator
	scram      *mongo.ScramSha256
}

// New creates a new Server.
func New(pool *pgxpool.Pool, cfg *config.Config) (*Server, error) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.MongoPort))
	if err != nil {
		return nil, fmt.Errorf("failed to listen on port %d: %w", cfg.MongoPort, err)
	}

	scram, err := mongo.NewScramSha256(cfg.MongoUser, cfg.MongoPassword)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize auth: %w", err)
	}

	return &Server{
		listener:   listener,
		pool:       pool,
		config:     cfg,
		translator: translator.New(),
		scram:      scram,
	}, nil
}

// Start starts the server.
func (s *Server) Start(ctx context.Context) {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			logging.Logger().Error("failed to accept connection", zap.Error(err))
			continue
		}

		go s.handleConnection(ctx, conn)
	}
}

// Close closes the server.
func (s *Server) Close() {
	s.listener.Close()
}

func (s *Server) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	remoteAddr := conn.RemoteAddr().String()
	logging.Logger().Debug("accepted connection", zap.String("remote_addr", remoteAddr))

	handler := mongo.NewHandler(s.pool, s.translator, s.scram)
	ctx = mongo.WithRemoteAddr(ctx, remoteAddr)
	// Buffered IO reduces syscall overhead significantly under high QPS.
	r := getReader(conn)
	defer putReader(r)
	w := getWriter(conn)
	defer putWriter(w)
	for {
		header, body, err := mwire.ReadOp(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				logging.Logger().Debug("connection closed by client", zap.String("remote_addr", remoteAddr))
				return
			}
			var netErr *net.OpError
			if errors.As(err, &netErr) && netErr.Err.Error() == "read: connection reset by peer" {
				logging.Logger().Debug("connection reset by peer", zap.String("remote_addr", remoteAddr))
				return
			}
			logging.Logger().Error("failed to read operation", zap.Error(err), zap.String("remote_addr", remoteAddr))
			return
		}

		response, err := handler.Handle(ctx, header, body)
		if err != nil {
			logging.Logger().Error("failed to handle operation", zap.Error(err), zap.String("remote_addr", remoteAddr))
			continue
		}

		if response != nil {
			if _, err := w.Write(response); err != nil {
				var netErr *net.OpError
				if errors.As(err, &netErr) && (strings.Contains(netErr.Err.Error(), "connection reset by peer") || strings.Contains(netErr.Err.Error(), "broken pipe")) {
					logging.Logger().Debug("connection closed on write", zap.String("remote_addr", remoteAddr), zap.Error(netErr))
					return
				}
				logging.Logger().Error("failed to write response", zap.Error(err), zap.String("remote_addr", remoteAddr))
				return
			}
			if err := w.Flush(); err != nil {
				logging.Logger().Error("failed to flush response", zap.Error(err), zap.String("remote_addr", remoteAddr))
				return
			}
		}
	}
}
