package service

import (
	"reflect"
	"testing"
	"time"
)

func TestPageCommentsUsesTimeThenStableStoredOrdinal(t *testing.T) {
	t0 := time.Date(2026, 11, 3, 0, 0, 0, 0, time.UTC)
	comments := []Comment{
		{Body: "later", Time: t0.Add(time.Second)},
		{Body: "first tie", Time: t0},
		{Body: "second tie", Time: t0},
	}
	page, err := PageComments(comments, 1, nil)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != 2 || page.Next == nil {
		t.Fatalf("first keyset page: %+v, %v", page, err)
	}
	first := *page.Next
	page, err = PageComments(comments, 1, &first)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != 3 || page.Next == nil {
		t.Fatalf("equal-time keyset page: %+v, %v", page, err)
	}
	page, err = PageComments(comments, 1, page.Next)
	if err != nil || len(page.Items) != 1 || page.Items[0].ID != 1 || page.Next != nil {
		t.Fatalf("final keyset page: %+v, %v", page, err)
	}
	// Adding a later stored comment cannot move an earlier ordinal. A pinned
	// reader excludes it; even a live array resumes after the same tuple.
	comments = append(comments, Comment{Body: "new", Time: t0.Add(2 * time.Second)})
	page, err = PageComments(comments, 2, &first)
	if err != nil || len(page.Items) != 2 || !reflect.DeepEqual([]uint64{page.Items[0].ID, page.Items[1].ID}, []uint64{3, 1}) || page.Next == nil {
		t.Fatalf("appended comment displaced earlier keyset: %+v, %v", page, err)
	}
	if _, err := PageComments(comments, 2, &CommentPosition{Time: t0, ID: 1}); err == nil {
		t.Fatal("mismatched signed position was accepted")
	}
}

func TestPageCommentsBoundsAndEmptyThread(t *testing.T) {
	page, err := PageComments(nil, 0, nil)
	if err != nil || page.Items == nil || len(page.Items) != 0 || page.Next != nil {
		t.Fatalf("empty default page: %+v, %v", page, err)
	}
	if _, err := PageComments(nil, 501, nil); err == nil {
		t.Fatal("page over 500 was accepted")
	}
	if _, err := PageComments(nil, -1, nil); err == nil {
		t.Fatal("negative page limit was accepted")
	}
}
