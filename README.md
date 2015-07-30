# microblogtrack
This is my working directory for trec 2015 microblog track.
For descritpion about the tasks please refer to 2015 microblog track guildlines:
https://github.com/lintool/twitter-tools/wiki/TREC-2015-Track-Guidelines


de.mpii.microblogtrack.task.Processor is the main entrance for both tasks, and onlineprocessor is for live stream from tweet api, meanwhile offlineprocessor uses the offline data to simulate the online stream for debugging.

TODO: seperate the notification task and digest task, so that the ranking model could rank documents on all tweets retrieved for the whole day.
