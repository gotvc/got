package gotns_test

import (
	"context"
	"testing"

	"github.com/gotvc/got/src/gotns"
	"github.com/gotvc/got/src/gotns/internal/gotnsop"
	"github.com/gotvc/got/src/internal/stores"
)

func TestMachine_FulfillObligations(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		// Named input parameters for target function.
		s       stores.RW
		state   gotns.State
		secret  *gotnsop.Secret
		want    bool
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := gotns.New()
			_, gotErr := m.FulfillObligations(context.Background(), tt.s, tt.state, tt.name, tt.secret)
			if gotErr != nil {
				if !tt.wantErr {
					t.Errorf("FulfillObligations() failed: %v", gotErr)
				}
				return
			}
			if tt.wantErr {
				t.Fatal("FulfillObligations() succeeded unexpectedly")
			}
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("FulfillObligations() = %v, want %v", gotErr, tt.want)
			}
		})
	}
}
