package kagent

import (
	"testing"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
)

func TestMatchTaskTreatsUnfinishedTasksAsPossiblyHoldingTheMessage(t *testing.T) {
	message := &a2atype.Message{ID: "orka-message"}
	done := &a2atype.Task{ID: "earlier", Status: a2atype.TaskStatus{State: a2atype.TaskStateCompleted}}
	// kagent shows a task whose first save was terminal as submitted, with its
	// input withheld from public history until native cleanup ends.
	withheld := &a2atype.Task{ID: "withheld", Status: a2atype.TaskStatus{State: a2atype.TaskStateSubmitted}}
	found := &a2atype.Task{ID: "found", History: []*a2atype.Message{message}, Status: a2atype.TaskStatus{State: a2atype.TaskStateWorking}}

	if task, unfinished := matchTask([]*a2atype.Task{done, withheld, found}, message.ID); task != found || unfinished {
		t.Fatalf("match = %v %v", task, unfinished)
	}
	if task, unfinished := matchTask([]*a2atype.Task{done, withheld}, message.ID); task != nil || !unfinished {
		t.Fatalf("an unfinished task did not make the lookup ambiguous: %v %v", task, unfinished)
	}
	if task, unfinished := matchTask([]*a2atype.Task{done, nil}, message.ID); task != nil || unfinished {
		t.Fatalf("finished tasks alone = %v %v", task, unfinished)
	}
}
