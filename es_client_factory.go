package awses

import (
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/elasticsearchservice"
)

type awsregion string
type sessionCache struct {
	sess *session.Session
	conf *aws.Config
}

// An Elasticsearch client factory with a cache that allows concurrent cached client sharing
type ElasticsearchClientFactory struct {
	Role string

	mutex   sync.RWMutex
	clients map[awsregion]sessionCache
}

func NewElasticsearchClientFactory(role string) *ElasticsearchClientFactory {
	return &ElasticsearchClientFactory{
		Role: role,
	}
}

// Returns a new client (does not lock or use the cache)
func (f *ElasticsearchClientFactory) New(region string) sessionCache {
	config := &aws.Config{
		Region: &region,
	}

	sess := session.Must(session.NewSession(config))

	if f.Role != "" {
		config.Credentials = stscreds.NewCredentials(sess, f.Role)
	}

	return sessionCache{
		sess: sess,
		conf: config,
	}
}

// Returns a cached client or instantiates a new client and caches it
func (f *ElasticsearchClientFactory) Get(region string) *elasticsearchservice.ElasticsearchService {
	// read lock to check client cache
	client, found := f.cached(region)
	if found {
		return elasticsearchservice.New(client.sess, client.conf)
	}

	// write lock to construct a new client (if necessary)
	f.mutex.Lock()
	defer f.mutex.Unlock()

	if f.clients == nil {
		f.clients = map[awsregion]sessionCache{}
	}

	client = f.New(region)
	f.clients[awsregion(region)] = client
	return elasticsearchservice.New(client.sess, client.conf)
}

func (f *ElasticsearchClientFactory) cached(region string) (sc sessionCache, found bool) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	if f.clients == nil {
		return sessionCache{}, false
	}

	sc = f.clients[awsregion(region)]
	if sc.conf != nil && sc.conf.Credentials != nil && sc.conf.Credentials.IsExpired() {
		delete(f.clients, awsregion(region))
		return sessionCache{}, false
	}
	return sc, true
}
