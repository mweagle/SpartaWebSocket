package main

import (
	"context"
	"encoding/json"
	"fmt"
	_ "net/http/pprof" // include pprop
	"os"
	"path/filepath"
	"strings"

	awsEvents "github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	apigwManagement "github.com/aws/aws-sdk-go/service/apigatewaymanagementapi"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	sparta "github.com/mweagle/Sparta"
	spartaAWS "github.com/mweagle/Sparta/aws"
	spartaCF "github.com/mweagle/Sparta/aws/cloudformation"
	gocf "github.com/mweagle/go-cloudformation"
	"github.com/sirupsen/logrus"
)

const (
	envKeyTableName          = "CONNECTIONS_TABLENAME"
	ddbAttributeConnectionID = "connectionID"
)

type wsResponse struct {
	StatusCode int    `json:"statusCode"`
	Body       string `json:"body"`
}

func deleteConnection(connectionID string, ddbService *dynamodb.DynamoDB) error {
	delItemInput := &dynamodb.DeleteItemInput{
		TableName: aws.String(os.Getenv(envKeyTableName)),
		Key: map[string]*dynamodb.AttributeValue{
			ddbAttributeConnectionID: &dynamodb.AttributeValue{
				S: aws.String(connectionID),
			},
		},
	}
	_, delItemErr := ddbService.DeleteItem(delItemInput)
	return delItemErr
}

// Connect the client
func connectWorld(ctx context.Context, request awsEvents.APIGatewayWebsocketProxyRequest) (*wsResponse, error) {
	// Preconditions
	logger, _ := ctx.Value(sparta.ContextKeyLogger).(*logrus.Logger)
	sess := spartaAWS.NewSession(logger)
	dynamoClient := dynamodb.New(sess)

	// Operation
	putItemInput := &dynamodb.PutItemInput{
		TableName: aws.String(os.Getenv(envKeyTableName)),
		Item: map[string]*dynamodb.AttributeValue{
			ddbAttributeConnectionID: &dynamodb.AttributeValue{
				S: aws.String(request.RequestContext.ConnectionID),
			},
		},
	}
	_, putItemErr := dynamoClient.PutItem(putItemInput)
	if putItemErr != nil {
		return &wsResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf("Failed to connect: %s", putItemErr.Error()),
		}, nil
	}
	return &wsResponse{
		StatusCode: 200,
		Body:       "Connected.",
	}, nil
}

// Disconnect the client
func disconnectWorld(ctx context.Context, request awsEvents.APIGatewayWebsocketProxyRequest) (*wsResponse, error) {

	// Preconditions
	logger, _ := ctx.Value(sparta.ContextKeyLogger).(*logrus.Logger)
	sess := spartaAWS.NewSession(logger)
	dynamoClient := dynamodb.New(sess)

	// Operation
	delItemErr := deleteConnection(request.RequestContext.ConnectionID, dynamoClient)
	if delItemErr != nil {
		return &wsResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf("Failed to disconnect: %s", delItemErr.Error()),
		}, nil
	}
	return &wsResponse{
		StatusCode: 200,
		Body:       "Disconnected.",
	}, nil
}

// sendMessage to all the subscribers
func sendMessage(ctx context.Context,
	request awsEvents.APIGatewayWebsocketProxyRequest) (*wsResponse, error) {

	// Preconditions
	logger, _ := ctx.Value(sparta.ContextKeyLogger).(*logrus.Logger)
	sess := spartaAWS.NewSession(logger)
	endpointURL := fmt.Sprintf("%s/%s",
		request.RequestContext.DomainName,
		request.RequestContext.Stage)
	logger.WithField("Endpoint", endpointURL).Info("API Gateway Endpoint")
	dynamoClient := dynamodb.New(sess)
	apigwMgmtClient := apigwManagement.New(sess, aws.NewConfig().WithEndpoint(endpointURL))

	// Get the input request...
	var objMap map[string]*json.RawMessage
	unmarshalErr := json.Unmarshal([]byte(request.Body), &objMap)
	if unmarshalErr != nil || objMap["data"] == nil {
		return &wsResponse{
			StatusCode: 500,
			Body:       "Failed to unmarshal request: " + unmarshalErr.Error(),
		}, nil
	}
	// Operations
	scanCallback := func(output *dynamodb.ScanOutput, lastPage bool) bool {
		// Send the message to all the clients
		for _, eachItem := range output.Items {
			receiverConnection := ""
			if eachItem[ddbAttributeConnectionID].S != nil {
				receiverConnection = *eachItem[ddbAttributeConnectionID].S
			}
			postConnectionInput := &apigwManagement.PostToConnectionInput{
				ConnectionId: aws.String(receiverConnection),
				Data:         *objMap["data"],
			}
			_, respErr := apigwMgmtClient.PostToConnectionWithContext(ctx, postConnectionInput)
			if respErr != nil {
				if receiverConnection != "" &&
					strings.Contains(respErr.Error(), apigwManagement.ErrCodeGoneException) {
					// Async clean it up...
					go deleteConnection(receiverConnection, dynamoClient)
				} else {
					logger.WithField("Error", respErr).Warn("Failed to post to connection")
				}
			}
		}
		return true
	}

	// Scan the connections table
	scanInput := &dynamodb.ScanInput{
		TableName: aws.String(os.Getenv(envKeyTableName)),
	}
	scanItemErr := dynamoClient.ScanPagesWithContext(ctx,
		scanInput,
		scanCallback)
	if scanItemErr != nil {
		return &wsResponse{
			StatusCode: 500,
			Body:       fmt.Sprintf("Failed to send message: %s", scanItemErr.Error()),
		}, nil
	}
	// Respond to the sender that data was sent
	return &wsResponse{
		StatusCode: 200,
		Body:       "Data sent.",
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// Main
func main() {
	// StackName
	pathName, _ := os.Getwd()
	dirName := strings.Split(pathName, string(filepath.Separator))

	sess := session.Must(session.NewSession())
	awsName, awsNameErr := spartaCF.UserAccountScopedStackName(dirName[len(dirName)-1],
		sess)
	if awsNameErr != nil {
		fmt.Print("Failed to create stack name\n")
		os.Exit(1)
	}
	// 1. Lambda Functions
	lambdaConnect, _ := sparta.NewAWSLambda("ConnectWorld",
		connectWorld,
		sparta.IAMRoleDefinition{})
	lambdaDisconnect, _ := sparta.NewAWSLambda("DisconnectWorld",
		disconnectWorld,
		sparta.IAMRoleDefinition{})
	lambdaSend, _ := sparta.NewAWSLambda("SendMessage",
		sendMessage,
		sparta.IAMRoleDefinition{})

	// APIv2 Websockets
	stage, _ := sparta.NewAPIV2Stage("v1")
	stage.Description = "New deploy!"

	apiGateway, _ := sparta.NewAPIV2(sparta.Websocket,
		"sample",
		"$request.body.message",
		stage)
	apiv2ConnectRoute, _ := apiGateway.NewAPIV2Route("$connect",
		lambdaConnect)
	apiv2ConnectRoute.OperationName = "ConnectRoute"
	apiv2DisconnectRoute, _ := apiGateway.NewAPIV2Route("$disconnect",
		lambdaDisconnect)
	apiv2DisconnectRoute.OperationName = "DisconnectRoute"

	apiv2SendRoute, _ := apiGateway.NewAPIV2Route("sendmessage",
		lambdaSend)
	apiv2SendRoute.OperationName = "SendRoute"

	var apigwPermissions = []sparta.IAMRolePrivilege{
		{
			Actions: []string{"execute-api:ManageConnections"},
			Resource: gocf.Join("",
				gocf.String("arn:aws:execute-api:"),
				gocf.Ref("AWS::Region"),
				gocf.String(":"),
				gocf.Ref("AWS::AccountId"),
				gocf.String(":"),
				gocf.Ref(apiGateway.LogicalResourceName()),
				gocf.String("/*")),
		},
	}
	lambdaSend.RoleDefinition.Privileges = append(lambdaSend.RoleDefinition.Privileges, apigwPermissions...)

	// Create the connection table decorator to provision the table and hook
	// up the environment variables
	decorator, _ := apiGateway.NewConnectionTableDecorator(envKeyTableName,
		ddbAttributeConnectionID,
		5,
		5)
	var lambdaFunctions []*sparta.LambdaAWSInfo
	lambdaFunctions = append(lambdaFunctions,
		lambdaConnect,
		lambdaDisconnect,
		lambdaSend)
	annotateErr := decorator.AnnotateLambdas(lambdaFunctions)
	if annotateErr != nil {
		os.Exit(2)
	}
	// Set everything up and run it...
	workflowHooks := &sparta.WorkflowHooks{
		ServiceDecorators: []sparta.ServiceDecoratorHookHandler{decorator},
	}
	err := sparta.MainEx(awsName,
		"Sparta application that demonstrates API v2 Websocket support",
		lambdaFunctions,
		apiGateway,
		nil,
		workflowHooks,
		false)
	if err != nil {
		os.Exit(1)
	}
}
