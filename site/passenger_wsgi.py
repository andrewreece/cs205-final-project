import sys, os
INTERP = "/home/dharmahound/marcus.andrewgarrettreece.com/anaconda/bin/python"
if sys.executable != INTERP:
    os.execl(INTERP, INTERP, *sys.argv)
sys.path.append(os.getcwd())
from run import app as application

# Uncomment next two lines to enable debugging
from werkzeug.debug import DebuggedApplication
application = DebuggedApplication(application, evalex=True)