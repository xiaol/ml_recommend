sh!/usr/bin/expect
set timeout 10
spawn ssh root@120.55.88.11
expect {
    "*yes/no*" {
        send "yes\n";
        exp_continue;
    }
    "*password:*" {
        send "Huohua123qdzx\r";
    }
}    
interact
