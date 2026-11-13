package clusters

// Broker is one broker of a cluster as its Gateway describes it.
type Broker struct {
	Cluster    string `json:"cluster"`
	ID         int32  `json:"id"`
	Host       string `json:"host"`
	Port       int32  `json:"port"`
	Controller bool   `json:"controller"`
}
