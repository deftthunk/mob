Mob

Very quick log parser using Perl's powerful regular expression engine. Threads work per file listed on CLI. Originally written to parse collections of PIX firewall logs.

Handles ASCII and GZIP'ed files

Use:
mob <regex> <log file(s)>
cat <log file(s)> | mob <regex>
