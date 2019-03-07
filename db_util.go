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

// LoadFromCli will load the PostgreSQL configuration using the function input variables.
func (this *PGConfig) LoadFromCLI(host, username, password, database string, port int32, disableSSL bool) {
	this.Username = username
	this.DefaultDBName = database
	this.Pass = password
	this.Host = fmt.Sprintf("%s:%d", host, port)

	// Set SSL Mode
	if disableSSL {
		this.SSLMode = "disable"
	} else {
		this.SSLMode = "require"
	}
}

// LoadFromEnv uses environment variables to load the PGConfig.
func (this *PGConfig) LoadFromEnv(debugNum int64, prefix, suffix string) {
	this.Username = viper.GetString(prefix + "USER" + suffix)
	this.Pass = viper.GetString(prefix + "PASS" + suffix)
	this.Host = viper.GetString(prefix + "HOST" + suffix)
	port := viper.GetString(prefix + "PORT" + suffix)
	this.DefaultDBName = viper.GetString(prefix + "NAME" + suffix)
	this.SSLMode = viper.GetString(prefix + "SSL" + suffix)

	// Set Port
	if port != "" {
		this.Host = strings.Join([]string{this.Host, port}, ":")
	}

	if this.Host == "" {
		log.Fatal("no database host provided")
	}
}

// DSN will construct the data source name from the supplied data in the PGConfig.
// See: https://en.wikipedia.org/wiki/Data_source_name
func (this *PGConfig) DSN() string {
	dsn := this.queryBaseDsn()

	if len(this.DefaultDBName) > 0 {
		dsn += "/" + this.DefaultDBName
	}
	if len(this.SSLMode) > 0 {
		if len(this.DefaultDBName) == 0 {
			dsn += "/"
		}

		dsn += "?sslmode=" + this.SSLMode
	}

	return dsn
}

// BaseDSN will return the base of the DSN in string form.
func (this *PGConfig) BaseDSN() string {
	dsn := this.queryBaseDsn()

	if len(this.SSLMode) > 0 {
		dsn += "/?sslmode=" + this.SSLMode
	}

	return dsn
}

// queryBaseDSN will return the base DSN constructed from the data in the PGConfig.
func (this *PGConfig) queryBaseDsn() string {
	var u *url.Userinfo
	if this.Pass != "" {
		u = url.UserPassword(this.Username, this.Pass)
	} else if this.Username != "" {
		u = url.User(this.Username)
	}

	return (&url.URL{
		Scheme: "postgres",
		User:   u,
		Host:   this.Host,
	}).String()
}

// URI returns a URI constructed from the supplied PGConfig.
func (this *PGConfig) URI() string {
	return this.DSN()
}

// BaseURI will return the BaseDSN for the supplied PGConfig.
func (this *PGConfig) BaseURI() string {
	return this.BaseDSN()
}

// OpenDB will open the database set in the PGConfig and return a pointer to the database connection.
func OpenDB(conf PGConfig) (*sql.DB, error) {
	dburl := conf.URI()
	db, err := sql.Open("postgres", dburl)
	if err != nil {
		log.Error(err)
		log.Debug("dburl: ", dburl)
		return nil, err
	}
	return db, nil
}
