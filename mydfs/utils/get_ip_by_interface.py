import psutil

def get_ip_by_interface(interface_name="eno1"):
    # Obter informações sobre todas as interfaces de rede
    interfaces = psutil.net_if_addrs()
    
    # Verificar se a interface existe
    if interface_name in interfaces:
        # Procurar pelo endereço IPv4 (geralmente o IP da máquina)
        for snic in interfaces[interface_name]:
            if snic.family == 2:  # 2 corresponde ao IPv4
                return snic.address
    else:
        return "localhost"
    
if __name__ == "__main__":
    print(get_ip_by_interface("eno1"))