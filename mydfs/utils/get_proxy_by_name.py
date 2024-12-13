import Pyro5.api

def get_proxy_by_name(name_service: str) -> Pyro5.api.Proxy:
  try:
    ns = Pyro5.api.locate_ns()
    return Pyro5.api.Proxy(ns.lookup(name_service))
  except Exception as e:
    print(f"Failed to locate nameserver: {e}")
    exit(1)