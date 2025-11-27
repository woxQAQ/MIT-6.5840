#!/usr/bin/env nu

def main [testType: string] {

    for $x in 1..5 {
        print $"Running ($testType) - iteration ($x)/5"

        # Use timeout to prevent hanging
        let output = (do -i {
            ^go test -run $testType | complete
        })

        if $output.exit_code != 0 {
            print $"Test failed on iteration ($x)"
            print $"Stdout: ($output.stdout)"
            $output.stdout | save --force err.log
            # print $"Stderr: ($output.stderr)"
            return $output.exit_code
        }

        print $"✓ Passed iteration ($x)"
    }

    print $"All 5 iterations passed!"
}
