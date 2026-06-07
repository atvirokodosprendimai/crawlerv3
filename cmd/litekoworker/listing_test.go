package main

import (
	"os"
	"testing"
)

func TestParseListingRealFixture(t *testing.T) {
	body, err := os.ReadFile("../../tmp/liteko/internal/crawler/testdata/listing_real.html")
	if err != nil {
		t.Skipf("fixture not available: %v", err)
	}
	lp, err := parseListing(body)
	if err != nil {
		t.Fatalf("parseListing: %v", err)
	}
	if len(lp.Cases) != 50 {
		t.Errorf("cases: want 50, got %d", len(lp.Cases))
	}
	for i, c := range lp.Cases {
		if c.Href == "" {
			t.Errorf("case[%d] href empty", i)
		}
	}
	if lp.ViewState == "" {
		t.Errorf("ViewState empty")
	}
	if lp.ViewStateGen == "" {
		t.Errorf("ViewStateGen empty")
	}
}

func TestPageButton(t *testing.T) {
	cases := []struct {
		i    int
		want string
	}{
		{0, ""},
		{1, "01"},
		{10, "10"},
		{11, "02"},
		{20, "11"},
		{21, "02"},
	}
	for _, c := range cases {
		if got := pageButton(c.i); got != c.want {
			t.Errorf("pageButton(%d) = %q, want %q", c.i, got, c.want)
		}
	}
}
