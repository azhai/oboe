// Pulls every library package into the test run so `odin test tests
// -all-packages` exercises the package-local test suites.
package tests

import _ "../cache"
import _ "../common"
import _ "../logdb"
import _ "../netio"
import _ "../queue"
import _ "../web"
