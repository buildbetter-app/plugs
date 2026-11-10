# Prepared Plug caller; explicit --binding is mandatory.
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent))
from host_drivers import main
main('load')
