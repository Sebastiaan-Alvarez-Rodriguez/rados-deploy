# In case we use this module with remoto legacy connections (local, ssh), we need this footer.
if __name__ == '__channelexec__':
    for item in channel:
        channel.send(eval(item))