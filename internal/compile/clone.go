package compile

import (
	"google.golang.org/protobuf/proto"

	client "github.com/bpalermo/sortie/gen/api/client"
)

func cloneOptions(o *client.CommandLineOptions) *client.CommandLineOptions {
	return proto.Clone(o).(*client.CommandLineOptions)
}
