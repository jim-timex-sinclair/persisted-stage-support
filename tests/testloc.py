import sys
import os

print(sys.path)
print(sys.path[0])

repo_path = os.path.split(sys.path[0])[0]
  
# Now add the various levels for search purposes
# adding src to path for testing
sys.path.insert(1, os.path.join(repo_path, 'src'))
# adding hpfc_autodade to path for testing.
sys.path.insert(1, os.path.join(repo_path, 'src', 'hpfc_getinfameta'))