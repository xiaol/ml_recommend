#!/usr/bin/expect
set timeout 10
spawn ssh 10.172.64.2
expect {
    "*yes/no*" {
        send "yes\n";
        exp_continue;
    }
    "*password:*" {
        send "Huohua123\r";
    }
}    
interact
