import Pyro5.api

def get_proxy_by_name(name_service: str) -> Pyro5.api.Proxy:
  try:
    ns = Pyro5.api.locate_ns()    
  except Exception as e:
    e.add_note("Failed to locate nameserver")
    raise e
  else:
    try:
      uri = ns.lookup(name_service)
    except Exception as e:
      e.add_note(f"Failed to lookup service {name_service}")
      raise e
    else:
      return Pyro5.api.Proxy(uri)