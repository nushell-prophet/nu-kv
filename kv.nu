# nushell kv
# The original version by @clipplerblood
# https://discord.com/channels/601130461678272522/615253963645911060/1149709351821516900

alias "core get" = get

# Returns the KV store as a table.
#
# Example:
# > kv
# ╭────┬──────╮
# │ pi │ 3.14 │
# ╰────┴──────╯
export def main [
    --files
] {
    if $files {
        ls (kvPath --values_folder)
    } else {
        load-kv
        | items {|k v| {name: $k, filename: $v}}
        | insert modified {|i|
            ls $i.filename | core get 0.modified
        }
        | sort-by modified --reverse
        | update modified {date humanize}
        | select name modified
    }
}

def kvPath [
    --values_folder # Return the path to the values folder
] null -> path {
    $nu.home-path
    | path join .config nushell kv (
        if $values_folder { 'values' } else { 'kv.nuon' }
    )
}

# Loads the KV store, creating it if it doesn't exist.
def load-kv [] : nothing -> record {
    if not (kvPath | path exists) {
        mkdir (kvPath --values_folder)
        {} | save (kvPath)
    }
    open (kvPath)
}

# Sets a value in the KV store. Any value can be provided, even other tables.
#
# > kv set pi 3.14
# > 3.14 | kv set pi
export def set [
    key: string = 'last'    # Key to set
    value?: any             # Value to set. Can be omitted if `kv set <key>` is used in a pipeline
    -p                      # Output the input value back to the pipeline
    --extension (-e): string = '' # extension for file format for saving
] any -> any {
    let $input = $in
    let $v = if $value == null {$input} else {$value}

    let type = $value | describe
    let $extension = if $extension != '' {
            $extension
        } else if $type =~ 'table|list|record|binary' {
            'msgpackz'
        } else if $type == string {
            'json' # msgpackz can't store primitives in 0.97.1
        } else {
            'nuon'
        }

    let $file_path = kvPath --values_folder
        | path join $'($key)_(date_now).($extension)'

    $v | save $file_path

    load-kv
    | if $key in $in {
        reject $key # to sort keys in chronological order
    } else {}
    | insert $key $file_path
    | save -f (kvPath)

    if $p { return $input }
}

# Gets a value from the KV store.
# If the key is missing, it returns null.
#
# > kv get pi
# 3.14
export def get [
    key: string@'nu-complete-key-names' = 'last'
] {
    load-kv
    | if $key not-in $in {
        return
    } else {
        core get $key | open
    }
}

# Get a file with a given name. Useful for opening previous versions of stored values.
export def get-file [
    filename: string@'nu-complete-file-names'
] {
    kvPath --values_folder | path join $filename | open
}

# Deletes a key from the KV store.
export def del [
    key: string@'nu-complete-key-names' = 'last'
] {
    load-kv
    | if $key not-in $in {
        return
    } else {
        reject $key | save -f (kvPath)
    }
}

# reset kv store (leave all files in the `values` folder)
export def reset [] {
    [false true]
    | input list 'confirm'
    | if $in {
        {} | save -f (kvPath)
    }
}

# Pushes a value to a list in the KV store.
# Notes:
#   - When pushing to a new key, a new list is created.
#   - When pushing to an existing key, the old value is checked to be of type list.
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
    value?, # Value to set. Can be omitted if `kv set <key>` is used in a pipeline
    -p,     # Output the input value back to the pipeline
    -u      # Push and ensure uniqueness, similar to a "sorted hash set". The pushed value will still be the last
] {
    let $piped = $in
    let $db = load-kv
    let $v = if $value != null { $value } else if $piped != null { $piped } else { return }

    if not ($key in $db) {
        # If key not in db, set a list with the value
        $db | upsert $key [$v] | save -f (kvPath)
    } else {
        # Otherwise, ensure the stored value is a list
        let $stored = $db | core get $key
        if not ($stored | describe | str starts-with list) {
            error make { msg: $"($key) is not a list \n($stored | table)" }
        }

        # Store the updated list. If -u is set, remove duplicates
        if $u {
            $db | upsert $key ($stored | where {|x| $x != $v} | append $v) | save -f (kvPath)
        } else {
            $db | upsert $key ($stored | append $v) | save -f (kvPath)
        }
    }

    if $p { return $v }
}

def date_now [] { date now | format date "%Y%m%d_%H%M%S_%f" }

def nu-complete-key-names [] {
    main
    | rename value description
}

def nu-complete-file-names [] {
    ls -s (kvPath --values_folder) | sort-by modified -r | select name modified | upsert modified {|i| $i.modified | date humanize} | rename value description
}
