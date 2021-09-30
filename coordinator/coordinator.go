package coordinator

import "github.com/zackwine/nanodm"

type CoordinatorHandler interface {
	// Called when a new client/source comes online
	Registered(server *Server, sourceName string, objects []nanodm.Object) error
	// Called when a a client/source goes offline
	Unregistered(server *Server, sourceName string, objects []nanodm.Object) error
	// Called when a client/source updates its supported objects
	UpdateObjects(server *Server, sourceName string, objects []nanodm.Object) error
}
