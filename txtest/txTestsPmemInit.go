///////////////////////////////////////////////////////////////////////
// Copyright 2018-2019 VMware, Inc.
// SPDX-License-Identifier: BSD-3-Clause
///////////////////////////////////////////////////////////////////////

package txtest

import (
	"os"

	"github.com/vmware/go-pmem-transaction/pmem"
)

func init() {
	os.Remove("tx_testFile")
	pmem.Init("tx_testFile")
}
