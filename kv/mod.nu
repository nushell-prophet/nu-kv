export use commands.nu [
    ls,
    set,
    get,
    get-file,
    del,
    reset,
    push,
    pop,
    init
]

export def main [] {ls}
# kv module respects $env.kv.path for locating it's cache
