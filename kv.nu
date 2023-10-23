# nushell kv
# the original version by @clipplerblood
# https://discord.com/channels/601130461678272522/615253963645911060/1149709351821516900

# Returns the KV store as table
# Example:
# > kv
# ╭────┬──────╮
# │ pi │ 3.14 │
# ╰────┴──────╯
export def main [] { load-kv }

export def kvPath [
    --values_folder # return the path to the values folder
] null => path {
    $nu.home-path | path join .config nushell kv
    | if $values_folder {
        path join values
    } else {
        path join kv.nuon
    }
}

# Loads the KV Store, creating it if it doesn't exist
def load-kv [] {
    if not (kvPath | path exists) {
        mkdir (kvPath --values_folder)
        {} | save (kvPath)
    }
    open (kvPath)
}


# Sets a value in the KV Store. Any value can be provided, even other tables
# Examples:
# > kv set pi 3.14
# > 3.14 | kv set pi
export def set [
    key: string = 'last'    # Key to set
    value?: any             # Value to set. Can be omitted if `kv set <k>` is used in a pipeline
    -p                      # Output back the input value to the pipeline
] {
    let $piped_in = $in
    let $v = $value | default $piped_in

    let $file_path = (kvPath --values_folder) | path join ($key + (date_now) + .json)

    $v | save $file_path

    (load-kv) | upsert $key $file_path | save -f (kvPath)

    if $p { return $v }
}

alias "core get" = get

# Gets a value from the KV Store
# If the key is missing, it returns null
# Example:
# > kv get pi
# 3.14
export def get [
    key: string = 'last'
] {
    let db = (load-kv)
    if not ($key in $db) {
        return
    }
    $db | core get $key | open $in
}


# Deletes a key from the KV Store
export def del [key] {
    let db = (load-kv)
    if not ($key in $db) {
        return
    }
    $db | reject $key | save -f (kvPath)
}


# Push a value to a list in the KV store.
# Notes:
#   - When pushing to a new key, a new list is created.
#   - When pushing to an already existing key, the old value is checked to be of type list.
# Example:
# > kv push my-stack "hello"
# > kv get my-stack
# [hello]
# > kv push my-stack "world"
# [hello, world]
# kv push my-stack "hello" -u
# [world, hello]
export def "push" [
    key,    # Key to set
    value?, # Value to set. Can be omitted if `kv set <k>` is used in a pipeline
    -p,     # Output back the input value to the pipeline
    -u      # Pushes and forces unicity, kinda like a "sorted hash set". The pushed value will still be the last
] {
    # Vars
    let $piped = $in
    let $db = (load-kv)
    let v = if $value != null { $value } else if $piped != null { $piped } else { return }

    if not ($key in $db) {
        # If key not in db, simply set a list with the value
        $db | upsert $key [$v] | save -f (kvPath)
    } else {
        # Otherwise, assert that the value is a list
        let stored = ($db | core get $key)
        if not ($stored | describe | str starts-with list) {
            error make {msg: $"($key) is not a list \n($stored | table )", }
        }

        # Store the pushed list. If -u unique flag, also remove the duplicates
        if $u {
            $db | upsert $key ($stored | where {|x| $x != $v} | append $v) | save -f (kvPath)
        } else {
            $db | upsert $key ($stored | append $v) | save -f (kvPath)
        }
    }

    if $p { return $v }
}

def date_now [] {date now | format date "%Y%m%d_%H%M%S"}