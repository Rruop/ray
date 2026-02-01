# Replaced with the current commit when building the wheels.
commit = "{{RAY_COMMIT_SHA}}"
version = "2.55.1+kuaishou.{{RAY_COMMIT_SHA_SHORT}}"

if __name__ == "__main__":
    print("%s %s" % (version, commit))
