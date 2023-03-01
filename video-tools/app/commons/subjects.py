"""List of initialized RxPy Subjects """

from rx.subject import Subject

# this subject recieves messages from stream reader
request_subject = Subject()
# this subject recieves successful messages after they are processed
event_subject = Subject()
# this subject recieves messages if there is an error while they are processed
error_subject = Subject()

# this subject recieves only boolean values to represent if the worker is
# leader or not
leader_subject = Subject()
