package ai_cli_protocol

import "regexp"

#Request: {
    cmd: string
    rid?: string & regexp.Match("^[a-zA-Z0-9-]{1,256}$")
    dry?: bool
    ...
}

#SuccessResponse: {
    ok: true
    rid?: string
    t: int & >0
    ms: int & >=0
    d: _
    next: string
    state: {
        total: int & >=0
        active: int & >=0
        ...
    }
}

#ErrorResponse: {
    ok: false
    rid?: string
    t: int & >0
    ms: int & >=0
    err: {
        code: string
        msg: string
        ctx?: {
            [string]: _
        }
    }
    fix: string
}

#Response: #SuccessResponse | #ErrorResponse
