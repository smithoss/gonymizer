package gonymizer

import (
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	_ "github.com/lib/pq" // make sure we load the driver
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// PGConfig is the main configuration structure for different PostgreSQL server configurations.
type PGConfig struct {
	Username string
	Pass     string
	Host     string

	DefaultDBName string

	SSLMode string
}

// LoadFromCLI will load the PostgreSQL configuration using the function input variables.
func (conf *PGConfig) LoadFromCLI(host, username, password, database string, port int32, disableSSL bool) {
	conf.Username = username
	conf.DefaultDBName = database
	conf.Pass = password
	conf.Host = fmt.Sprintf("%s:%d", host, port)

	// Set SSL Mode
	if disableSSL {
		conf.SSLMode = "disable"
	} else {
		conf.SSLMode = "require"
	}
}

// LoadFromEnv uses environment variables to load the PGConfig.
func (conf *PGConfig) LoadFromEnv(debugNum int64, prefix, suffix string) {
	conf.Username = viper.GetString(prefix + "USER" + suffix)
	conf.Pass = viper.GetString(prefix + "PASS" + suffix)
	conf.Host = viper.GetString(prefix + "HOST" + suffix)
	port := viper.GetString(prefix + "PORT" + suffix)
	conf.DefaultDBName = viper.GetString(prefix + "NAME" + suffix)
	conf.SSLMode = viper.GetString(prefix + "SSL" + suffix)

	// Set Port
	if port != "" {
		conf.Host = strings.Join([]string{conf.Host, port}, ":")
	}

	if conf.Host == "" {
		log.Fatal("no database host provided")
	}
}

// DSN will construct the data source name from the supplied data in the PGConfig.
// See: https://en.wikipedia.org/wiki/Data_source_name
func (conf *PGConfig) DSN() string {
	dsn := conf.queryBaseDsn()

	if len(conf.DefaultDBName) > 0 {
		dsn += "/" + conf.DefaultDBName
	}
	if len(conf.SSLMode) > 0 {
		if len(conf.DefaultDBName) == 0 {
			dsn += "/"
		}

		dsn += "?sslmode=" + conf.SSLMode
	}

	return dsn
}

// BaseDSN will return the base of the DSN in string form.
func (conf *PGConfig) BaseDSN() string {
	dsn := conf.queryBaseDsn()

	if len(conf.SSLMode) > 0 {
		dsn += "/?sslmode=" + conf.SSLMode
	}

	return dsn
}

// queryBaseDSN will return the base DSN constructed from the data in the PGConfig.
func (conf *PGConfig) queryBaseDsn() string {
	var u *url.Userinfo
	if conf.Pass != "" {
		u = url.UserPassword(conf.Username, conf.Pass)
	} else if conf.Username != "" {
		u = url.User(conf.Username)
	}

	return (&url.URL{
		Scheme: "postgres",
		User:   u,
		Host:   conf.Host,
	}).String()
}

// URI returns a URI constructed from the supplied PGConfig.
func (conf *PGConfig) URI() string {
	return conf.DSN()
}

// BaseURI will return the BaseDSN for the supplied PGConfig.
func (conf *PGConfig) BaseURI() string {
	return conf.BaseDSN()
}

// OpenDB will open the database set in the PGConfig and return a pointer to the database connection.
func OpenDB(conf PGConfig) (*sql.DB, error) {
	dburl := conf.URI()
	db, err := sql.Open("postgres", dburl)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	return db, nil
}
