package producer

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringPtrToString(t *testing.T) {
	var param *string

	actual := stringPtrToString(param)
	assert.Equal(t, "<nil>", actual)
}
