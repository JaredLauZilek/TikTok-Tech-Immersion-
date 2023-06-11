package main

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/kitex_gen/rpc"
	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/kitex_gen/rpc/imservice"
	"github.com/TikTokTechImmersion/assignment_demo_2023/http-server/proto_gen/api"
	"github.com/cloudwego/hertz"
	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
	"github.com/cloudwego/hertz/pkg/server/httpx"
	"github.com/cloudwego/kitex/client"
	etcd "github.com/kitex-contrib/registry-etcd"
)

var cli imservice.Client

func main() {
	r, err := etcd.NewEtcdResolver([]string{"etcd:2379"})
	if err != nil {
		log.Fatal(err)
	}

	cli = imservice.MustNewClient("demo.rpc.server",
		client.WithResolver(r),
		client.WithRPCTimeout(1*time.Second),
		client.WithHostPorts("rpc-server:8888"),
	)

	router := hertz.NewRouter()

	router.GET("/ping", func(ctx context.Context, w http.ResponseWriter, r *http.Request) {
		app.JSON(w, consts.StatusOK, map[string]interface{}{"message": "pong"})
	})

	router.POST("/api/send", sendMessage)
	router.GET("/api/pull", pullMessage)

	server := httpx.Server{
		Handler: router,
	}

	log.Fatal(server.ListenAndServe(":8080"))
}

func sendMessage(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req api.SendRequest
	if err := app.ParseJSONRequest(r, &req); err != nil {
		app.ErrorResponse(w, consts.StatusBadRequest, "Failed to parse request body: %v", err)
		return
	}

	resp, err := cli.Send(ctx, &rpc.SendRequest{
		Message: &rpc.Message{
			Chat:   req.Chat,
			Text:   req.Text,
			Sender: req.Sender,
		},
	})
	if err != nil {
		app.ErrorResponse(w, consts.StatusInternalServerError, err.Error())
		return
	} else if resp.Code != 0 {
		app.ErrorResponse(w, consts.StatusInternalServerError, resp.Msg)
		return
	}

	app.JSON(w, consts.StatusOK, nil)
}

func pullMessage(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	var req api.PullRequest
	if err := app.ParseQuery(r, &req); err != nil {
		app.ErrorResponse(w, consts.StatusBadRequest, "Failed to parse request parameters: %v", err)
		return
	}

	resp, err := cli.Pull(ctx, &rpc.PullRequest{
		Chat:    req.Chat,
		Cursor:  req.Cursor,
		Limit:   req.Limit,
		Reverse: &req.Reverse,
	})
	if err != nil {
		app.ErrorResponse(w, consts.StatusInternalServerError, err.Error())
		return
	} else if resp.Code != 0 {
		app.ErrorResponse(w, consts.StatusInternalServerError, resp.Msg)
		return
	}

	messages := make([]*api.Message, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		messages = append(messages, &api.Message{
			Chat:     msg.Chat,
			Text:     msg.Text,
			Sender:   msg.Sender,
			SendTime: msg.SendTime,
		})
	}

	app.JSON(w, consts
