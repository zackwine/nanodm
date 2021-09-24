package collector

import "github.com/zackwine/nanodm"

type CollectorHandler interface {
	ClientRegistered(sourceName string, objects []nanodm.Object) error
	ClientRemoved(sourceName string, objects []nanodm.Object) error
	GetObjects(objectNames []string) (objects []nanodm.Object, err error)
	SetObjects(objects []nanodm.Object) error
}
