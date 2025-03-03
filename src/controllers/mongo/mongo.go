package mongo

import (
	"context"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
)

/* Loggers */
var logger log.Logger = log.NewLogfmtLogger(os.Stdout)

var (
	Client     *mongo.Client
	Collection *mongo.Collection
)

type key string

const (
	hostKey     = key("hostKey")
	usernameKey = key("usernameKey")
	passwordKey = key("passwordKey")
	//databaseKey = key("databaseKey")
)

func init() {
	var err error
	ctx := context.Background()
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ctx = context.WithValue(ctx, hostKey, os.Getenv("MONGO_HOST"))
	ctx = context.WithValue(ctx, usernameKey, os.Getenv("MONGO_USERNAME"))
	ctx = context.WithValue(ctx, passwordKey, os.Getenv("MONGO_PASSWORD"))

	err = configDB(ctx)
	if err != nil {
		level.Error(logger).Log("msg", "Database configuration failed, mongo client", "ts", log.DefaultTimestampUTC(), "err", err)
	}

	level.Info(logger).Log("msg", "Successfully connected to MongoDB", "ts", log.DefaultTimestampUTC())
}

func configDB(ctx context.Context) error {
	var err error

	uri := fmt.Sprintf(`mongodb://%s:%s@%s`,
		ctx.Value(usernameKey).(string),
		ctx.Value(passwordKey).(string),
		ctx.Value(hostKey).(string),
	)

	Client, err = mongo.NewClient(options.Client().ApplyURI(uri))

	if err != nil {
		return fmt.Errorf("couldn't connect to mongo: %v", err)

	}
	err = Client.Connect(ctx)
	if err != nil {
		return fmt.Errorf("client couldn't connect with context: %v", err)
	}

	return nil
}
