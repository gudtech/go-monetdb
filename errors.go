package monetdb

import (
	"fmt"
	"strings"
	"sync"
)

type LockedErrs struct {
	errors []error
	mutex  sync.Mutex
}

func NewLockedErrors() *LockedErrs {
	var mutex sync.Mutex
	return &LockedErrs{
		errors: []error{},
		mutex:  mutex,
	}
}

func (errs *LockedErrs) AddError(err error) {
	errs.mutex.Lock()
	defer errs.mutex.Unlock()
	errs.errors = append(errs.errors, err)
}

func (errs *LockedErrs) GetErrors() error {
	errs.mutex.Lock()
	defer errs.mutex.Unlock()

	if len(errs.errors) > 0 {
		var errorStrings []string
		for _, err := range errs.errors {
			if err != nil {
				errorStrings = append(errorStrings, err.Error())
			}
		}
		return fmt.Errorf("%s", strings.Join(errorStrings, ", "))
	}

	return nil
}
