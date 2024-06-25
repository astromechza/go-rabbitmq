package rabbitmq

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const enableDockerIntegrationTestsFlag = `ENABLE_DOCKER_INTEGRATION_TESTS`

func prepareDockerTest(t *testing.T) (connStr string) {
	if v, ok := os.LookupEnv(enableDockerIntegrationTestsFlag); ok && strings.ToUpper(v) != "TRUE" {
		t.Skipf("integration tests are only run if '%s' is TRUE", enableDockerIntegrationTestsFlag)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	out, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--detach", "--publish=5672:5672", "--quiet", "--", "rabbitmq:3-alpine").Output()
	if err != nil {
		t.Log("container id", string(out))
		t.Fatalf("error launching rabbitmq in docker: %v", err)
	}
	t.Cleanup(func() {
		t.Log("hi")
		containerId := strings.TrimSpace(string(out))
		t.Logf("attempting to shutdown container '%s'", containerId)
		if err := exec.Command("docker", "rm", "--force", containerId).Run(); err != nil {
			t.Logf("failed to stop: %v", err)
		}
	})
	return "amqp://guest:guest@localhost:5672/"
}

func waitForHealthyAmqp(t *testing.T, connStr string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	tkr := time.NewTicker(time.Second)

	for {
		select {
		case <-ctx.Done():
			t.Fatal("timed out waiting for healthy amqp", ctx.Err())
		case <-tkr.C:
			if err := func() error {
				t.Log("attempting connection")
				conn, err := NewConn(connStr)
				if err != nil {
					return fmt.Errorf("failed to setup connection: %v", err)
				}
				defer conn.Close()

				pub, err := NewPublisher(conn)
				if err != nil {
					return fmt.Errorf("failed to setup publisher: %v", err)
				}

				t.Log("attempting publish")
				return pub.PublishWithContext(ctx, []byte{}, []string{"ping"}, WithPublishOptionsExchange(""))
			}(); err != nil {
				t.Log("publish ping failed", err.Error())
			} else {
				t.Log("ping successful")
				return
			}
		}
	}
}

func TestSimplePubSub(t *testing.T) {
	connStr := prepareDockerTest(t)
	waitForHealthyAmqp(t, connStr)

	conn, err := NewConn(connStr)
	if err != nil {
		t.Fatal("error creating connection", err)
	}
	defer conn.Close()

	t.Logf("new consumer")
	consumer, err := NewConsumer(conn, "my_queue")
	if err != nil {
		t.Fatal("error creating consumer", err)
	}
	defer consumer.Close()

	go func() {
		err = consumer.Run(func(d Delivery) Action {
			log.Printf("consumed: %v", string(d.Body))
			return Ack
		})
		if err != nil {
			t.Log("consumer run failed", err)
		}
	}()

	t.Logf("new publisher")
	publisher, err := NewPublisher(conn)
	if err != nil {
		t.Fatal("error creating publisher", err)
	}
	publisher.NotifyPublish(func(p Confirmation) {
		return
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	t.Logf("new publish")
	confirms, err := publisher.PublishWithDeferredConfirmWithContext(
		ctx, []byte("example"), []string{"my_queue"},
		WithPublishOptionsMandatory,
	)
	if err != nil {
		t.Fatal("failed to publish", err)
	}
	for _, confirm := range confirms {
		if _, err := confirm.WaitContext(ctx); err != nil {
			t.Fatal("failed to wait for publish", err)
		}
	}
	t.Logf("success")

}
