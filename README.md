├── README.md
├── go.mod
├── go.sum
├── cmd
│   └── appctl
│       └── main.go          # Entry point for CLI commands
├── config
│   └── config.go            # Configuration settings
├── messages
│    ├── broadcast.go          
│    ├── broadcast_list.go      
│    └── service.go
├── pkg                     # Shared packages
│   └── logger
│       └── logger.go               
├── queue
│   ├── rabbitmq.go          
└── internal
    └── adapter
        ├── rabbit.go         # Rabbit adapter for external interactions



The application’s main resides in the cmd package, which will be used in compiling and generating the application’s executable. This could be initializing a REST API in cmd/api or calls via CLI (Command Line Interface) in cmd/appctl. The main of each application should also contain the service startup and required dependency injections.(for external interaction)

The internal directory combines all the implementations related to protocols used to interact with the application’s clients. In the example above, an HTTP communication layer was developed using the framework gin-gonic in internal/http/gin, where handler. go receives the service and creates the routes that expose the operations, associating them with the handler functions implemented in scheduler service. This package is responsible for translating HTTP requests to what the messenger service understands, being an adapter in the Hexagonal Architecture.

The event layer also reflects an external interaction, in this case, events that arrive or are published in a message, being an adapter for this actor driver. In the example, Kafka publishes and consumes events on one or more topics.