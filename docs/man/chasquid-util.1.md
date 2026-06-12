# NAME

chasquid-util - chasquid management tool

# SYNOPSIS

**chasquid-util** \[_options_\] user-add _user@domain_ \[--password=_password_\] \[--receive\_only\]

**chasquid-util** \[_options_\] user-remove _user@domain_

**chasquid-util** \[_options_\] user-list _domain_ \[--receive\_only\] \[--prefix=_prefix_\]

**chasquid-util** \[_options_\] authenticate _user@domain_ \[--password=_password_\]

**chasquid-util** \[_options_\] check-userdb _domain_

**chasquid-util** \[_options_\] aliases-resolve _addr_

**chasquid-util** \[_options_\] domaininfo-remove _domain_

**chasquid-util** \[_options_\] print-config

# DESCRIPTION

chasquid-util is a command-line utility for [chasquid(1)](chasquid.1.md) operations.

# OPTIONS

- **user-add** _user@domain_ \[--password=_password_\] \[--receive\_only\]

    Add a new user to the domain.

    If _--receive\_only_ is given, then the user will never successfully
    authenticate. This is useful when creating receive-only users.

- **user-remove** _user@domain_

    Remove the user from the domain.

- **user-list** _domain_ \[--receive\_only\] \[--prefix=_prefix_\]

    List the users in the domain, sorted by name. Receive-only users (see
    _--receive\_only_ in **user-add**) are marked as such in the output.

    If _--receive\_only_ is given, only receive-only users are listed. If
    _--prefix_ is given, only users whose name starts with the given prefix are
    listed.

- **authenticate** _user@domain_ \[--password=_password_\]

    Check the user's password.

- **check-userdb** _domain_

    Check the integrity of the domain's users database.

- **aliases-resolve** _addr_

    Resolve the given address. Talks to the running chasquid instance.

- **domaininfo-remove** _domain_

    Remove the domain information entry. This can be used to manually allow a
    security level downgrade. Talks to the running chasquid instance.

- **print-config**

    Parse and print the configuration in a human-readable way.

- **-C** or **--config\_dir=&lt;path**>

    Configuration directory.

- **--configdir=&lt;path**>

    Deprecated alias for **--config\_dir=&lt;path**>.

# SEE ALSO

[chasquid(1)](chasquid.1.md)
