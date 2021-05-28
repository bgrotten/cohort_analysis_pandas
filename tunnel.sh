#!/bin/sh
ssh -f -i ~/.ssh/id_rsa benno_grottenegg@<ip-address> -L port:link:port -N
