#!/usr/bin/expect
set timeout 10
spawn ssh root@120.27.157.234
expect {
    "*yes/no*" {
        send "yes\n";
        exp_continue;
    }
    "*password:*" {
        send "@Spider!%&Server@\r";
    }
}    
interact
