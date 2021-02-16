if __name__ == "__main__":
    channels = [chn.strip() for chn in open("channels.list", "r").readlines()]
    tmpl = open("app_deploy.yml.tmpl", "r").read()

    with open("app_deploy.yml", "w") as f:
        for chn in channels:
            f.write(tmpl.replace("<<nats-channel>>", f'"{chn}"'))
