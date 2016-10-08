package swift

import (
	"net/url"
	"os"
	"restic/errors"
	"strings"
)

// Config contains basic configuration needed to specify swift location for an
// swift server
type Config struct {
	Domain                 string
	DomainId               string
	UserName               string
	ApiKey                 string
	AuthUrl                string
	Region                 string
	Tenant                 string
	TenantId               string
	TrustId                string
	StorageUrl             string
	AuthToken              string
	Container              string
	Prefix                 string
	DefaultContainerPolicy string
}

func ParseConfig(s string) (interface{}, error) {

	url, err := url.Parse(s)
	if err != nil {
		return nil, errors.Wrap(err, "url.Parse")
	}

	if url.Host != "" {
		return nil, errors.New("swift: Hostname in swift url is not supported")
	}

	parts := strings.SplitN(url.Path, "/", 3)
	if len(parts) < 2 || len(parts[1]) == 0 {
		return nil, errors.New("swift: Missing container name")
	}

	cfg := Config{
		Container:              parts[1],
		Domain:                 os.Getenv("SWIFT_API_DOMAIN"),
		DomainId:               os.Getenv("SWIFT_API_DOMAIN_ID"),
		UserName:               os.Getenv("SWIFT_API_USER"),
		ApiKey:                 os.Getenv("SWIFT_API_KEY"),
		AuthUrl:                os.Getenv("SWIFT_AUTH_URL"),
		Region:                 os.Getenv("SWIFT_REGION_NAME"),
		Tenant:                 os.Getenv("SWIFT_TENANT"),
		TenantId:               os.Getenv("SWIFT_TENANT_ID"),
		TrustId:                os.Getenv("SWIFT_TRUST_ID"),
		StorageUrl:             os.Getenv("SWIFT_URL"),
		AuthToken:              os.Getenv("SWIFT_AUTH_TOKEN"),
		DefaultContainerPolicy: os.Getenv("SWIFT_DEFAULT_CONTAINER_POLICY"),
	}

	if len(parts) > 2 {
		cfg.Prefix = parts[2]
	}

	return cfg, nil
}
