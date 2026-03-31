package jsonrepair

type FixJSONRequest struct {
	JsonString string
}

type FixJSONResponse struct {
	RepairedJSON string
	Valid        bool
	Errors       []string
}
