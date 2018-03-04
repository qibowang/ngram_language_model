#!/bin/bash

startOrder=1
endOrder=4
tasks=1
isLLzo=1
input=/user/wangqibo/data/coupus/seg0/lishijisuan_5/2013080113_02.txt.txt_out
rawCountPath=/user/wangqibo/leftRightRawcount/${endOrder}gram/

hadoop jar leftRightRawcount.jar -input $input -rawcount $rawCountPath -startOrder $startOrder -endOrder $endOrder -tasks $tasks -isLzo $isLzo
