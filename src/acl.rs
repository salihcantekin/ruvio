//! Config-file ACL users plus a process-wide `ACL SETUSER` directory.
//! `ACL WHOAMI`, `ACL USERS`, `ACL LIST`, `ACL GETUSER`, `ACL CAT`, and `ACL SETUSER`.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use crate::protocol::{
    Command, GeoCommand, HotCommand, SetOp, acl_canonical, acl_commands, glob_match,
};

use crate::routing::command_keys;
use crate::security::{SecurityConfig, constant_time_eq};

const NOPERM_KEYS: &[u8] = b"NOPERM this user has no permissions to access one or more keys";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclUser {
    pub name: Vec<u8>,
    pub enabled: bool,
    pub nopass: bool,
    /// BLAKE3 digests of the accepted passwords, never the passwords themselves.
    pub passwords: Vec<[u8; 32]>,
    pub rules: AclRules,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AclRules {
    pub all_commands: bool,
    pub commands: BTreeSet<&'static str>,
    pub all_keys: bool,
    pub key_patterns: Vec<Vec<u8>>,
    /// `parent|sub` names denied while `parent` itself is allowed (`+acl -acl|setuser`).
    /// Only ever non-empty with `all_commands` off, so the allow-all check stays first.
    pub denied_subcommands: BTreeSet<&'static str>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectionAuth {
    pub authenticated: bool,
    /// Shared so a command forwarded to another shard carries the caller's identity without a copy.
    pub username: Arc<[u8]>,
    pub rules: Arc<AclRules>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AclDeny {
    Command(&'static str),
    Key,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AuthError {
    NoPasswordConfigured,
    WrongPass,
}

impl AclRules {
    pub fn allow_all() -> Self {
        Self {
            all_commands: true,
            commands: BTreeSet::new(),
            all_keys: true,
            key_patterns: Vec::new(),
            denied_subcommands: BTreeSet::new(),
        }
    }

    pub fn none() -> Self {
        Self {
            all_commands: false,
            commands: BTreeSet::new(),
            all_keys: false,
            key_patterns: Vec::new(),
            denied_subcommands: BTreeSet::new(),
        }
    }

    pub fn allows_command(&self, name: &str) -> bool {
        if self.all_commands {
            return true;
        }

        if self.commands.contains(name) {
            return true;
        }

        if let Some((parent, _)) = name.split_once('|') {
            return self.commands.contains(parent) && !self.denied_subcommands.contains(name);
        }

        false
    }

    pub fn allows_key(&self, key: &[u8]) -> bool {
        self.all_keys
            || self
                .key_patterns
                .iter()
                .any(|pattern| glob_match(pattern, key))
    }

    pub fn authorize(&self, command: &Command<'_>) -> Result<(), AclDeny> {
        let name = command_acl_name(command);

        if !self.allows_command(name) {
            return Err(AclDeny::Command(name));
        }

        if self.all_keys {
            return Ok(());
        }

        if command_keys(command)
            .into_iter()
            .any(|key| !self.allows_key(key))
            || exposes_any_key_name(command)
        {
            return Err(AclDeny::Key);
        }

        Ok(())
    }

    pub fn authorize_hot(&self, command: &HotCommand<'_>) -> Result<(), AclDeny> {
        let (name, key) = match command {
            HotCommand::Ping(_) => ("ping", None),
            HotCommand::Get(key) => ("get", Some(*key)),
            HotCommand::Set(key, _, _) => ("set", Some(*key)),
            HotCommand::SetEx(key, _, _) => ("set", Some(*key)),
            HotCommand::Delete(key) => ("del", Some(*key)),
            HotCommand::Increment(key, _) => ("incr", Some(*key)),
        };

        if !self.allows_command(name) {
            return Err(AclDeny::Command(name));
        }

        if let Some(key) = key
            && !self.allows_key(key)
        {
            return Err(AclDeny::Key);
        }

        Ok(())
    }
}

impl ConnectionAuth {
    pub fn from_security(security: &SecurityConfig) -> Self {
        let default = security
            .directory()
            .find_user(b"default")
            .unwrap_or_else(|| AclUser::default_user(&security.requirepass));

        if default.enabled && default.nopass {
            Self {
                authenticated: true,
                username: default.name.into(),
                rules: Arc::new(default.rules),
            }
        } else {
            Self {
                authenticated: false,
                username: Arc::from(&[][..]),
                rules: Arc::new(AclRules::none()),
            }
        }
    }
}

/// Live user list shared by every clone of one `SecurityConfig`.
#[derive(Debug)]
pub struct AclDirectory {
    users: Mutex<Vec<AclUser>>,
}

impl AclDirectory {
    pub(crate) fn from_users(users: Vec<AclUser>) -> Self {
        Self {
            users: Mutex::new(users),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<AclUser>> {
        self.users.lock().unwrap_or_else(|error| error.into_inner())
    }

    pub fn find_user(&self, name: &[u8]) -> Option<AclUser> {
        self.lock()
            .iter()
            .find(|user| user.name.eq_ignore_ascii_case(name))
            .cloned()
    }

    pub fn snapshot(&self) -> Vec<AclUser> {
        self.lock().clone()
    }

    pub fn authenticate(
        &self,
        username: Option<&[u8]>,
        password: &[u8],
    ) -> Result<AclUser, AuthError> {
        let users = self.lock();

        if !users.iter().any(AclUser::has_password) {
            return Err(AuthError::NoPasswordConfigured);
        }

        let requested = username.unwrap_or(b"default");
        let mut matched = None;
        let mut dummy = false;

        for user in users.iter() {
            if user.name.eq_ignore_ascii_case(requested) {
                if user.enabled && user.password_matches(password) {
                    matched = Some(user.clone());
                } else {
                    let _ = user.password_matches(password);
                }
            } else {
                dummy |= user.password_matches(password);
            }
        }

        let _ = dummy;

        matched.ok_or(AuthError::WrongPass)
    }

    /// Applies rules onto an existing user, or creates a disabled user with no permissions.
    /// A failed rule leaves the directory unchanged.
    pub fn set_user(&self, name: &[u8], rules: &[&[u8]]) -> Result<(), String> {
        validate_user_name(name)?;
        let mut users = self.lock();
        let mut draft = users
            .iter()
            .find(|user| user.name.eq_ignore_ascii_case(name))
            .cloned()
            .unwrap_or_else(|| AclUser {
                name: name.to_vec(),
                enabled: false,
                nopass: false,
                passwords: Vec::new(),
                rules: AclRules::none(),
            });

        for rule in rules {
            let text =
                std::str::from_utf8(rule).map_err(|_| "ACL rule must be UTF-8".to_string())?;

            apply_rule(&mut draft, text)?;
        }

        if draft.nopass {
            draft.passwords.clear();
        }

        if let Some(user) = users
            .iter_mut()
            .find(|user| user.name.eq_ignore_ascii_case(name))
        {
            *user = draft;
        } else {
            users.push(draft);
        }

        Ok(())
    }

    pub fn replace_users(&self, users: Vec<AclUser>) {
        *self.lock() = users;
    }
}

fn password_digest(password: &[u8]) -> [u8; 32] {
    *blake3::hash(password).as_bytes()
}

fn hex_digest(digest: &[u8; 32]) -> String {
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn parse_hex_digest(text: &str) -> Option<[u8; 32]> {
    if text.len() != 64 || !text.is_ascii() {
        return None;
    }

    let mut digest = [0u8; 32];

    for (slot, pair) in digest.iter_mut().zip(text.as_bytes().chunks(2)) {
        *slot = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }

    Some(digest)
}

fn add_digest(user: &mut AclUser, digest: [u8; 32]) {
    if !user.passwords.contains(&digest) {
        user.passwords.push(digest);
    }
}

impl AclUser {
    pub fn default_user(requirepass: &str) -> Self {
        Self {
            name: b"default".to_vec(),
            enabled: true,
            nopass: requirepass.is_empty(),
            passwords: if requirepass.is_empty() {
                Vec::new()
            } else {
                vec![password_digest(requirepass.as_bytes())]
            },
            rules: AclRules::allow_all(),
        }
    }

    pub fn has_password(&self) -> bool {
        !self.nopass && !self.passwords.is_empty()
    }

    pub fn password_matches(&self, password: &[u8]) -> bool {
        if self.nopass {
            return true;
        }

        let mut matched = false;
        let digest = password_digest(password);

        for candidate in &self.passwords {
            matched |= constant_time_eq(&digest, candidate);
        }

        matched
    }

    pub fn parse(name: &str, rules: &str) -> Result<Self, String> {
        validate_user_name(name.as_bytes())?;

        let mut user = Self {
            name: name.as_bytes().to_vec(),
            enabled: false,
            nopass: false,
            passwords: Vec::new(),
            rules: AclRules::none(),
        };

        for token in rules.split_whitespace() {
            apply_rule(&mut user, token)?;
        }

        if user.nopass {
            user.passwords.clear();
        }

        Ok(user)
    }

    pub fn describe(&self) -> String {
        let mut parts = vec![
            "user".into(),
            String::from_utf8_lossy(&self.name).into_owned(),
            if self.enabled { "on" } else { "off" }.into(),
        ];

        if self.nopass {
            parts.push("nopass".into());
        } else if self.passwords.is_empty() {
            parts.push("resetpass".into());
        } else {
            parts.push("***".into());
        }

        if self.rules.all_keys {
            parts.push("~*".into());
        } else if self.rules.key_patterns.is_empty() {
            parts.push("resetkeys".into());
        } else {
            for pattern in &self.rules.key_patterns {
                parts.push(format!("~{}", String::from_utf8_lossy(pattern)));
            }
        }

        if self.rules.all_commands {
            parts.push("+@all".into());
        } else if self.rules.commands.is_empty() {
            parts.push("-@all".into());
        } else {
            for command in &self.rules.commands {
                parts.push(format!("+{command}"));
            }
        }

        for denied in &self.rules.denied_subcommands {
            parts.push(format!("-{denied}"));
        }

        parts.join(" ")
    }

    /// One ACL file line, including passwords. `ACL LIST` keeps using `describe`.
    pub fn file_line(&self) -> Result<String, String> {
        let mut parts = vec![
            "user".into(),
            std::str::from_utf8(&self.name)
                .map_err(|_| "ACL user name must be UTF-8".to_string())?
                .to_string(),
            if self.enabled { "on" } else { "off" }.into(),
        ];

        if self.nopass {
            parts.push("nopass".into());
        } else if self.passwords.is_empty() {
            parts.push("resetpass".into());
        } else {
            for digest in &self.passwords {
                parts.push(format!("#{}", hex_digest(digest)));
            }
        }

        if self.rules.all_keys {
            parts.push("~*".into());
        } else if self.rules.key_patterns.is_empty() {
            parts.push("resetkeys".into());
        } else {
            for pattern in &self.rules.key_patterns {
                parts.push(format!("~{}", String::from_utf8_lossy(pattern)));
            }
        }

        if self.rules.all_commands {
            parts.push("+@all".into());
        } else if self.rules.commands.is_empty() {
            parts.push("-@all".into());
        } else {
            for command in &self.rules.commands {
                parts.push(format!("+{command}"));
            }
        }

        for denied in &self.rules.denied_subcommands {
            parts.push(format!("-{denied}"));
        }

        Ok(parts.join(" "))
    }
}

pub fn parse_acl_file(path: &std::path::Path) -> Result<Vec<AclUser>, String> {
    let text = std::fs::read_to_string(path).map_err(|error| format!("ACL file: {error}"))?;
    let mut users = Vec::new();
    let mut names = BTreeSet::new();

    for (index, line) in text.lines().enumerate() {
        let Some(user) = parse_acl_line(line)
            .map_err(|error| format!("ACL file line {}: {error}", index + 1))?
        else {
            continue;
        };

        let key = String::from_utf8_lossy(&user.name).to_ascii_lowercase();

        if !names.insert(key) {
            return Err(format!("ACL file line {}: duplicate user", index + 1));
        }

        users.push(user);
    }

    Ok(users)
}

pub fn write_acl_file(path: &std::path::Path, users: &[AclUser]) -> Result<(), String> {
    let mut body = String::new();

    for user in users {
        body.push_str(&user.file_line()?);
        body.push('\n');
    }

    // Password digests: owner-only, and never a torn file after a crash.
    crate::persistence::write_file_atomically(path, body.as_bytes(), true)
        .map_err(|error| format!("ACL file: {error}"))
}

fn parse_acl_line(line: &str) -> Result<Option<AclUser>, String> {
    let line = line.trim();

    if line.is_empty() || line.starts_with('#') {
        return Ok(None);
    }

    let mut parts = line.split_whitespace();
    let kind = parts.next().unwrap_or_default();

    if !kind.eq_ignore_ascii_case("user") {
        return Err("expected a user line".into());
    }

    let Some(name) = parts.next() else {
        return Err("user line is missing a name".into());
    };

    let rules = parts.collect::<Vec<_>>().join(" ");

    Ok(Some(AclUser::parse(name, &rules)?))
}

impl SecurityConfig {
    pub fn default_user(&self) -> AclUser {
        self.effective_users()
            .into_iter()
            .find(|user| user.name.eq_ignore_ascii_case(b"default"))
            .unwrap_or_else(|| AclUser::default_user(&self.requirepass))
    }

    pub fn effective_users(&self) -> Vec<AclUser> {
        self.with_default_user(self.users.clone())
    }

    pub(crate) fn with_default_user(&self, mut users: Vec<AclUser>) -> Vec<AclUser> {
        if !users
            .iter()
            .any(|user| user.name.eq_ignore_ascii_case(b"default"))
        {
            users.insert(0, AclUser::default_user(&self.requirepass));
        } else if !self.requirepass.is_empty() {
            for user in &mut users {
                if user.name.eq_ignore_ascii_case(b"default")
                    && !user
                        .passwords
                        .iter()
                        .any(|digest| *digest == password_digest(self.requirepass.as_bytes()))
                {
                    user.nopass = false;
                    user.passwords
                        .push(password_digest(self.requirepass.as_bytes()));
                }
            }
        }

        users
    }

    pub fn any_password_configured(&self) -> bool {
        self.effective_users().iter().any(AclUser::has_password)
    }

    pub fn authenticate(
        &self,
        username: Option<&[u8]>,
        password: &[u8],
    ) -> Result<AclUser, AuthError> {
        self.directory().authenticate(username, password)
    }
}

pub fn noperm_response(deny: AclDeny) -> crate::engine::Response<'static> {
    match deny {
        AclDeny::Command(name) => crate::engine::Response::OwnedError(
            format!(
                "NOPERM this user has no permissions to run the '{name}' command or its subcommand"
            )
            .into_bytes(),
        ),
        AclDeny::Key => crate::engine::Response::Error(NOPERM_KEYS),
    }
}

pub fn acl_response<'a>(
    security: &SecurityConfig,
    auth: &ConnectionAuth,
    command: &crate::protocol::AclCommand<'a>,
) -> crate::engine::Response<'static> {
    use crate::engine::Response;
    use crate::protocol::AclCommand;
    use std::borrow::Cow;
    match command {
        AclCommand::WhoAmI => Response::Bulk(Some(Cow::Owned(auth.username.to_vec()))),
        AclCommand::Users => Response::Array(
            security
                .directory()
                .snapshot()
                .into_iter()
                .map(|user| Some(Cow::Owned(user.name)))
                .collect(),
        ),
        AclCommand::List => Response::Array(
            security
                .directory()
                .snapshot()
                .into_iter()
                .map(|user| Some(Cow::Owned(user.describe().into_bytes())))
                .collect(),
        ),
        AclCommand::GetUser(name) => security
            .directory()
            .snapshot()
            .into_iter()
            .find(|user| user.name.eq_ignore_ascii_case(name))
            .map(|user| {
                let commands = if user.rules.all_commands {
                    "+@all".into()
                } else {
                    user.rules
                        .commands
                        .iter()
                        .map(|name| format!("+{name}"))
                        .chain(
                            user.rules
                                .denied_subcommands
                                .iter()
                                .map(|name| format!("-{name}")),
                        )
                        .collect::<Vec<_>>()
                        .join(" ")
                        .into_bytes()
                };

                let keys = if user.rules.all_keys {
                    b"~*".to_vec()
                } else {
                    user.rules
                        .key_patterns
                        .iter()
                        .map(|pattern| format!("~{}", String::from_utf8_lossy(pattern)))
                        .collect::<Vec<_>>()
                        .join(" ")
                        .into_bytes()
                };

                Response::Array(vec![
                    Some(Cow::Borrowed(b"flags")),
                    Some(Cow::Borrowed(if user.enabled { b"on" } else { b"off" })),
                    Some(Cow::Borrowed(b"passwords")),
                    Some(Cow::Owned(user.passwords.len().to_string().into_bytes())),
                    Some(Cow::Borrowed(b"commands")),
                    Some(Cow::Owned(commands)),
                    Some(Cow::Borrowed(b"keys")),
                    Some(Cow::Owned(keys)),
                ])
            })
            .unwrap_or(Response::Bulk(None)),
        AclCommand::Cat(None) => Response::Array(
            categories()
                .into_iter()
                .map(|name| Some(Cow::Borrowed(name.as_bytes())))
                .collect(),
        ),
        AclCommand::Cat(Some(name)) => {
            let requested = String::from_utf8_lossy(name).to_ascii_lowercase();
            let commands = commands_in_category(&requested);

            if commands.is_empty() {
                Response::OwnedError(format!("ERR Unknown category '{requested}'").into_bytes())
            } else {
                Response::Array(
                    commands
                        .into_iter()
                        .map(|command| Some(Cow::Borrowed(command.as_bytes())))
                        .collect(),
                )
            }
        }

        AclCommand::SetUser { name, rules } => {
            let borrowed = rules.iter().map(|rule| rule.as_ref()).collect::<Vec<_>>();

            match security.directory().set_user(name, &borrowed) {
                Ok(()) => Response::Simple(b"OK"),
                Err(error) => Response::OwnedError(format!("ERR {error}").into_bytes()),
            }
        }

        AclCommand::Load => acl_file_result(load_acl_directory(security)),
        AclCommand::Save => acl_file_result(save_acl_directory(security)),
    }
}

fn acl_file_result(result: Result<(), String>) -> crate::engine::Response<'static> {
    use crate::engine::Response;

    match result {
        Ok(()) => Response::Simple(b"OK"),
        Err(error) => Response::OwnedError(format!("ERR {error}").into_bytes()),
    }
}

/// `ACL LOAD` replaces the live directory. `ACL SAVE` writes it.
fn load_acl_directory(security: &SecurityConfig) -> Result<(), String> {
    if security.acl_file.as_os_str().is_empty() {
        return Err("ACL file is not configured".into());
    }

    let users = parse_acl_file(&security.acl_file)?;

    security
        .directory()
        .replace_users(security.with_default_user(users));

    Ok(())
}

fn save_acl_directory(security: &SecurityConfig) -> Result<(), String> {
    if security.acl_file.as_os_str().is_empty() {
        return Err("ACL file is not configured".into());
    }

    let users = security.directory().snapshot();

    write_acl_file(&security.acl_file, &users)
}

fn validate_user_name(name: &[u8]) -> Result<(), String> {
    if name.is_empty() || name.len() > 64 {
        return Err("ACL user name must be 1 to 64 bytes".into());
    }

    if name
        .iter()
        .any(|byte| !byte.is_ascii() || byte.is_ascii_whitespace())
    {
        return Err("ACL user name must be ASCII without whitespace".into());
    }

    Ok(())
}

fn apply_rule(user: &mut AclUser, token: &str) -> Result<(), String> {
    match token {
        "on" => user.enabled = true,
        "off" => user.enabled = false,
        "nopass" => {
            user.nopass = true;
            user.passwords.clear();
        }

        "resetpass" => {
            user.nopass = false;
            user.passwords.clear();
        }

        "allkeys" | "~*" => {
            user.rules.all_keys = true;
            user.rules.key_patterns.clear();
        }

        "resetkeys" => {
            user.rules.all_keys = false;
            user.rules.key_patterns.clear();
        }

        "allcommands" | "+@all" => {
            user.rules.all_commands = true;
            user.rules.commands.clear();
            user.rules.denied_subcommands.clear();
        }

        "nocommands" | "-@all" => {
            user.rules.all_commands = false;
            user.rules.commands.clear();
            user.rules.denied_subcommands.clear();
        }

        token if token.starts_with('>') => {
            let password = &token.as_bytes()[1..];

            if password.is_empty() || password.len() > 512 {
                return Err("ACL password must be 1 to 512 bytes".into());
            }

            user.nopass = false;
            add_digest(user, password_digest(password));
        }

        token if token.starts_with('#') => {
            let digest = parse_hex_digest(&token[1..])
                .ok_or_else(|| "ACL password hash must be 64 hex characters".to_string())?;

            user.nopass = false;
            add_digest(user, digest);
        }

        token if token.starts_with('<') => {
            let digest = password_digest(&token.as_bytes()[1..]);

            user.passwords.retain(|stored| *stored != digest);
        }

        token if token.starts_with('!') => {
            let digest = parse_hex_digest(&token[1..])
                .ok_or_else(|| "ACL password hash must be 64 hex characters".to_string())?;

            user.passwords.retain(|stored| *stored != digest);
        }

        token if token.starts_with('~') => {
            let pattern = token.as_bytes()[1..].to_vec();

            if pattern.is_empty() {
                return Err("ACL key pattern cannot be empty".into());
            }

            user.rules.all_keys = false;

            if !user.rules.key_patterns.contains(&pattern) {
                user.rules.key_patterns.push(pattern);
            }
        }

        token if token.starts_with("+@") => apply_category(user, &token[2..], true)?,
        token if token.starts_with("-@") => apply_category(user, &token[2..], false)?,
        token if token.starts_with('+') => apply_command(user, &token[1..], true)?,
        token if token.starts_with('-') => apply_command(user, &token[1..], false)?,
        _ => return Err(format!("unsupported ACL rule `{token}`")),
    }

    Ok(())
}

fn apply_category(user: &mut AclUser, category: &str, enable: bool) -> Result<(), String> {
    let category = category.to_ascii_lowercase();
    let commands = commands_in_category(&category);

    if commands.is_empty() {
        return Err(format!("unknown ACL category `@{category}`"));
    }

    for command in commands {
        apply_named_command(user, command, enable);
    }

    if enable && !user.rules.all_commands {
        for (subcommand, categories) in SUBCOMMAND_CATEGORIES {
            let (parent, _) = subcommand.split_once('|').expect("subcommand name");

            if !categories.contains(&category.as_str())
                && user.rules.commands.contains(parent)
                && !user.rules.commands.contains(subcommand)
            {
                user.rules.denied_subcommands.insert(subcommand);
            }
        }
    }

    Ok(())
}

fn apply_command(user: &mut AclUser, name: &str, enable: bool) -> Result<(), String> {
    let name = name.to_ascii_lowercase();

    if name.contains('|') {
        let (parent, _) = name.split_once('|').expect("split");

        if !is_known_command(parent) {
            return Err(format!("unknown ACL command `{name}`"));
        }

        let known =
            known_subcommand(&name).ok_or_else(|| format!("unknown ACL subcommand `{name}`"))?;

        apply_named_command(user, known, enable);

        return Ok(());
    }

    let canonical = acl_canonical(&name).ok_or_else(|| format!("unknown ACL command `{name}`"))?;

    apply_named_command(user, canonical, enable);
    Ok(())
}

fn apply_named_command(user: &mut AclUser, name: &'static str, enable: bool) {
    let rules = &mut user.rules;
    let subcommand = name.contains('|');

    if enable {
        if subcommand {
            rules.denied_subcommands.remove(name);
        } else {
            // `+parent` allows every subcommand again.
            rules.denied_subcommands.retain(|denied| {
                denied
                    .split_once('|')
                    .is_none_or(|(parent, _)| parent != name)
            });
        }

        if !rules.all_commands {
            rules.commands.insert(name);
        }

        return;
    }

    if rules.all_commands {
        rules.commands = all_command_names();
        rules.all_commands = false;
    }

    rules.commands.remove(name);

    if subcommand {
        // The parent may stay allowed; the denial is what keeps this subcommand out.
        rules.denied_subcommands.insert(name);
    } else {
        rules.commands.retain(|allowed| {
            allowed
                .split_once('|')
                .is_none_or(|(parent, _)| parent != name)
        });
        rules.denied_subcommands.retain(|denied| {
            denied
                .split_once('|')
                .is_none_or(|(parent, _)| parent != name)
        });
    }
}

/// Every `parent|sub` name [`command_acl_name`] can return. A rule on any other subcommand
/// could never match a command, so it is refused rather than stored.
const ACL_SUBCOMMANDS: &[&str] = &[
    "acl|cat",
    "acl|getuser",
    "acl|list",
    "acl|load",
    "acl|save",
    "acl|setuser",
    "acl|users",
    "acl|whoami",
    "client|kill",
    "client|list",
    "config|get",
    "config|rewrite",
    "config|set",
    "ruvio|bigkeys",
    "ruvio|hotkeys",
    "ruvio|lease",
    "ruvio|migrate",
    "ruvio|moveslots",
    "ruvio|topology",
];

fn known_subcommand(name: &str) -> Option<&'static str> {
    ACL_SUBCOMMANDS.iter().copied().find(|known| *known == name)
}

fn all_command_names() -> BTreeSet<&'static str> {
    acl_commands().map(|spec| spec.name).collect()
}

fn is_known_command(name: &str) -> bool {
    acl_canonical(name).is_some()
}

fn categories() -> Vec<&'static str> {
    let mut names = BTreeSet::from(["all"]);

    for spec in acl_commands() {
        names.extend(spec.categories.iter().copied());
    }

    names.into_iter().collect()
}

/// Subcommands whose categories differ from their parent's. A category grants them only when
/// it lists them; granting the parent through another category leaves them denied. An explicit
/// `+parent` still grants every subcommand.
const SUBCOMMAND_CATEGORIES: &[(&str, &[&str])] = &[
    ("client|kill", &["admin", "slow", "dangerous"]),
    ("client|list", &["admin", "slow", "dangerous"]),
    ("ruvio|hotkeys", &["admin", "slow"]),
    ("ruvio|bigkeys", &["admin", "slow"]),
    ("ruvio|migrate", &["admin", "slow", "dangerous"]),
    ("ruvio|moveslots", &["admin", "slow", "dangerous"]),
    ("ruvio|topology", &["admin", "slow", "dangerous"]),
    ("ruvio|lease", &["admin", "slow", "dangerous"]),
];

fn commands_in_category(category: &str) -> Vec<&'static str> {
    if category == "all" {
        return acl_commands().map(|spec| spec.name).collect();
    }

    acl_commands()
        .filter(|spec| spec.categories.contains(&category))
        .map(|spec| spec.name)
        .chain(
            SUBCOMMAND_CATEGORIES
                .iter()
                .filter(|(_, categories)| categories.contains(&category))
                .map(|(name, _)| *name),
        )
        .collect()
}

/// Key names a key-restricted user must not see: the change feed, the key statistics and the
/// keyspace notification channels report keys of every pattern.
fn exposes_any_key_name(command: &Command<'_>) -> bool {
    match command {
        Command::Changes(_) | Command::ChangesSubscribe(_) => true,
        Command::Ruvio(Some(word)) => {
            word.eq_ignore_ascii_case(b"hotkeys") || word.eq_ignore_ascii_case(b"bigkeys")
        }
        Command::Subscribe(channels) | Command::ShardSubscribe(channels) => channels
            .iter()
            .any(|channel| is_notification_channel(channel)),
        Command::PSubscribe(patterns) => patterns
            .iter()
            .any(|pattern| pattern_may_match_notifications(pattern)),
        _ => false,
    }
}

const NOTIFICATION_PREFIXES: [&[u8]; 2] = [b"__keyspace@", b"__keyevent@"];

fn is_notification_channel(channel: &[u8]) -> bool {
    NOTIFICATION_PREFIXES
        .iter()
        .any(|prefix| channel.starts_with(prefix))
}

/// Conservative: true unless the pattern's literal prefix already rules out both prefixes.
fn pattern_may_match_notifications(pattern: &[u8]) -> bool {
    let literal = pattern
        .iter()
        .position(|byte| matches!(byte, b'*' | b'?' | b'[' | b'\\'))
        .map_or(pattern, |end| &pattern[..end]);

    NOTIFICATION_PREFIXES
        .iter()
        .any(|prefix| prefix.starts_with(literal) || literal.starts_with(prefix))
}

pub fn command_acl_name(command: &Command<'_>) -> &'static str {
    match command {
        Command::CreateTtl { inner, .. } => command_acl_name(inner),
        Command::Ping(_) => "ping",
        Command::CommandMeta { .. } => "command",
        Command::Wait { .. } => "wait",
        Command::Lease { .. } => "lease",
        Command::Semaphore { .. } => "semaphore",
        Command::Limit { .. } => "limit",
        Command::Once { .. } => "once",
        Command::IdempotencyBegin { .. } | Command::IdempotencyComplete { .. } => "idem",
        Command::Take { .. } => "take",
        Command::Transfer { .. } => "transfer",
        Command::HashTransfer { .. } => "htransfer",
        Command::SetIfEq { .. } | Command::SetIfCmp { .. } => "set",
        Command::DelIfEq { .. } => "delex",
        Command::ListPushCapped { back: false, .. } => "lpushcap",
        Command::ListPushCapped { back: true, .. } => "rpushcap",
        Command::GetEx { .. } => "getex",
        Command::Release { .. } => "release",
        Command::Renew { .. } => "renew",
        Command::SetVersioned { .. } => "setv",
        Command::Changes(_) | Command::ChangesSubscribe(_) => "changes",
        Command::Ruvio(Some(word)) if word.eq_ignore_ascii_case(b"hotkeys") => "ruvio|hotkeys",
        Command::Ruvio(Some(word)) if word.eq_ignore_ascii_case(b"bigkeys") => "ruvio|bigkeys",
        Command::Ruvio(_) => "ruvio",
        Command::Auth { .. } => "auth",
        Command::Acl(crate::protocol::AclCommand::WhoAmI) => "acl|whoami",
        Command::Acl(crate::protocol::AclCommand::Users) => "acl|users",
        Command::Acl(crate::protocol::AclCommand::List) => "acl|list",
        Command::Acl(crate::protocol::AclCommand::GetUser(_)) => "acl|getuser",
        Command::Acl(crate::protocol::AclCommand::Cat(_)) => "acl|cat",
        Command::Acl(crate::protocol::AclCommand::SetUser { .. }) => "acl|setuser",
        Command::Acl(crate::protocol::AclCommand::Load) => "acl|load",
        Command::Acl(crate::protocol::AclCommand::Save) => "acl|save",
        Command::ClusterSlots
        | Command::ClusterNodes
        | Command::ClusterInfo
        | Command::ClusterShards
        | Command::ClusterMyId
        | Command::ClusterKeySlot(_) => "cluster",
        Command::ReadOnly => "readonly",
        Command::ReadWrite => "readwrite",
        Command::ConfigGet(_) => "config|get",
        Command::ConfigSet(_) => "config|set",
        Command::ConfigRewrite => "config|rewrite",
        Command::ReplicaOf { .. } => "replicaof",
        Command::MoveSlots(_) => "ruvio|moveslots",
        Command::Topology(_) => "ruvio|topology",
        Command::PrimaryLease(_) => "ruvio|lease",
        Command::Migrate(_) => "ruvio|migrate",
        Command::Role => "role",
        Command::Info(_) => "info",
        Command::Save => "save",
        Command::Bgsave => "bgsave",
        Command::Get(_) => "get",
        Command::Set(_, _, _) => "set",
        Command::Increment(_, _) | Command::IncrementCapped { .. } => "incr",
        Command::MGet(_) => "mget",
        Command::MSet(_) => "mset",
        Command::GetSet(_, _) => "getset",
        Command::Append(_, _) => "append",
        Command::SetNx(..) => "setnx",
        Command::GetDel(_) => "getdel",
        Command::MSetNx(_) => "msetnx",
        Command::IncrementFloat(..) => "incrbyfloat",
        Command::GetRange { .. } => "getrange",
        Command::SetRange { .. } => "setrange",
        Command::SetBit { .. } => "setbit",
        Command::BitField {
            read_only_name: true,
            ..
        } => "bitfield_ro",
        Command::BitField { .. } => "bitfield",
        Command::GetBit(..) => "getbit",
        Command::BitCount { .. } => "bitcount",
        Command::BitPos { .. } => "bitpos",
        Command::BitOp { .. } => "bitop",
        Command::FillGet { .. } => "fillget",
        Command::FillSet { .. } => "fillset",
        Command::PfAdd { .. } => "pfadd",
        Command::PfCount(_) => "pfcount",
        Command::PfMerge { .. } => "pfmerge",
        Command::StrLen(_) => "strlen",
        Command::Exists(_) => "exists",
        Command::Type(_) => "type",
        Command::Rename { .. } => "rename",
        Command::RenameNx { .. } => "renamenx",
        Command::Keys(_) => "keys",
        Command::ClientList { .. } => "client|list",
        Command::ClientKill { .. } => "client|kill",
        Command::ClientId | Command::ClientInfo | Command::ClientSetInfo => "client",
        Command::Slowlog(_) => "slowlog",
        Command::LatencyLatest => "latency",
        Command::Echo(_) => "echo",
        Command::Object { .. } => "object",
        Command::MemoryUsage(_) => "memory",
        Command::RandomKey => "randomkey",
        Command::Touch(_) => "touch",
        Command::Dump(_) => "dump",
        Command::Restore { .. } => "restore",
        Command::ExpireTime { millis: false, .. } => "expiretime",
        Command::ExpireTime { millis: true, .. } => "pexpiretime",
        Command::Copy { .. } => "copy",
        Command::Scan { .. } => "scan",
        Command::DbSize => "dbsize",
        Command::Del(_) => "del",
        Command::ListPush(_, _) => "lpush",
        Command::ListPushBack(_, _) => "rpush",
        Command::ListPop { .. } => "lpop",
        Command::ListPopBack { .. } => "rpop",
        Command::ListLen(_) => "llen",
        Command::ListIndex(_, _) => "lindex",
        Command::ListTrim(_, _, _) => "ltrim",
        Command::ListRange(_, _, _) => "lrange",
        Command::ListBlock { back: false, .. } => "blpop",
        Command::ListBlock { back: true, .. } => "brpop",
        Command::ListInsert { .. } => "linsert",
        Command::ListSet { .. } => "lset",
        Command::ListRemove { .. } => "lrem",
        Command::ListPos { .. } => "lpos",
        Command::ListPushX { back: false, .. } => "lpushx",
        Command::ListPushX { back: true, .. } => "rpushx",
        Command::ListMove { block: None, .. } => "lmove",
        Command::ListMove { block: Some(_), .. } => "blmove",
        Command::ListMPop { block: None, .. } => "lmpop",
        Command::ListMPop { block: Some(_), .. } => "blmpop",
        Command::SetAdd(_, _) => "sadd",
        Command::SetRemove(_, _) => "srem",
        Command::SetContains(_, _) => "sismember",
        Command::SetCard(_) => "scard",
        Command::SetMembers(_) => "smembers",
        Command::SetPop { .. } => "spop",
        Command::SetRandom { .. } => "srandmember",
        Command::SetAlgebra {
            destination: None,
            op: SetOp::Inter,
            ..
        } => "sinter",
        Command::SetAlgebra {
            destination: None,
            op: SetOp::Union,
            ..
        } => "sunion",
        Command::SetAlgebra {
            destination: None,
            op: SetOp::Diff,
            ..
        } => "sdiff",
        Command::SetAlgebra {
            destination: Some(_),
            op: SetOp::Inter,
            ..
        } => "sinterstore",
        Command::SetAlgebra {
            destination: Some(_),
            op: SetOp::Union,
            ..
        } => "sunionstore",
        Command::SetAlgebra {
            destination: Some(_),
            op: SetOp::Diff,
            ..
        } => "sdiffstore",
        Command::SetMove { .. } => "smove",
        Command::SetMIsMember { .. } => "smismember",
        Command::SetInterCard { .. } => "sintercard",
        Command::SortedSetCount { .. } => "zcount",
        Command::SortedSetLexCount { .. } => "zlexcount",
        Command::SortedSetMScore { .. } => "zmscore",
        Command::SortedSetRandMember { .. } => "zrandmember",
        Command::HashRandField { .. } => "hrandfield",
        Command::HashIncrementFloat { .. } => "hincrbyfloat",
        Command::SortedSetAlgebra {
            op: SetOp::Union,
            destination: None,
            ..
        } => "zunion",
        Command::SortedSetAlgebra {
            op: SetOp::Inter,
            destination: None,
            ..
        } => "zinter",
        Command::SortedSetAlgebra {
            op: SetOp::Diff,
            destination: None,
            ..
        } => "zdiff",
        Command::SortedSetAlgebra {
            op: SetOp::Union,
            destination: Some(_),
            ..
        } => "zunionstore",
        Command::SortedSetAlgebra {
            op: SetOp::Inter,
            destination: Some(_),
            ..
        } => "zinterstore",
        Command::SortedSetAlgebra {
            op: SetOp::Diff,
            destination: Some(_),
            ..
        } => "zdiffstore",
        Command::HashSet { .. } => "hset",
        Command::HashMSet { .. } => "hmset",
        Command::HashDelete { .. } => "hdel",
        Command::HashGet { .. } => "hget",
        Command::HashMGet { .. } => "hmget",
        Command::HashLen(_) => "hlen",
        Command::HashGetAll(_) => "hgetall",
        Command::HashExists { .. } => "hexists",
        Command::HashKeys(_) => "hkeys",
        Command::HashValues(_) => "hvals",
        Command::HashIncrement { .. } => "hincrby",
        Command::HashSetNx { .. } => "hsetnx",
        Command::HashStrLen { .. } => "hstrlen",
        Command::HashScan { .. } => "hscan",
        Command::CollectionScan {
            kind: crate::protocol::CollectionScanKind::Set,
            ..
        } => "sscan",
        Command::CollectionScan {
            kind: crate::protocol::CollectionScanKind::SortedSet,
            ..
        } => "zscan",
        Command::SortedSetAdd { .. } => "zadd",
        Command::SortedSetRange { reverse: false, .. } => "zrange",
        Command::SortedSetRange { reverse: true, .. } => "zrevrange",
        Command::SortedSetRangeByScore {
            via_zrange: true, ..
        } => "zrange",
        Command::SortedSetRangeByScore { .. } => "zrangebyscore",
        Command::SortedSetRangeByLex {
            invocation: crate::protocol::LexInvocation::ZRange,
            ..
        } => "zrange",
        Command::SortedSetRangeByLex {
            invocation: crate::protocol::LexInvocation::ZRevRangeByLex,
            ..
        } => "zrevrangebylex",
        Command::SortedSetRangeByLex { .. } => "zrangebylex",
        Command::SortedSetRemove { .. } => "zrem",
        Command::SortedSetIncr { .. } => "zincrby",
        Command::SortedSetCard(_) => "zcard",
        Command::SortedSetScore { .. } => "zscore",
        Command::SortedSetRank { reverse: false, .. } => "zrank",
        Command::SortedSetRank { reverse: true, .. } => "zrevrank",
        Command::SortedSetPop { max: false, .. } => "zpopmin",
        Command::SortedSetPop { max: true, .. } => "zpopmax",
        Command::SortedSetPopByScore { .. } => "zpopbyscore",
        Command::SortedSetMoveDue { .. } => "zmovedue",
        Command::SortedSetBlockPop { max: false, .. } => "bzpopmin",
        Command::SortedSetBlockPop { max: true, .. } => "bzpopmax",
        Command::SortedSetRemoveRange {
            range: crate::protocol::SortedRemoveRange::Rank(..),
            ..
        } => "zremrangebyrank",
        Command::SortedSetRemoveRange {
            range: crate::protocol::SortedRemoveRange::Score(..),
            ..
        } => "zremrangebyscore",
        Command::SortedSetRemoveRange {
            range: crate::protocol::SortedRemoveRange::Lex(..),
            ..
        } => "zremrangebylex",
        Command::HashExpire { name, .. } => name,
        Command::HashTtl { reply, .. } => match reply {
            crate::protocol::FieldTtlReply::Seconds => "httl",
            crate::protocol::FieldTtlReply::Millis => "hpttl",
            crate::protocol::FieldTtlReply::AtSeconds => "hexpiretime",
            crate::protocol::FieldTtlReply::AtMillis => "hpexpiretime",
        },
        Command::HashPersist { .. } => "hpersist",
        Command::GeoAdd { .. } => "geoadd",
        Command::GeoDist { .. } => "geodist",
        Command::GeoPos { .. } => "geopos",
        Command::GeoHash { .. } => "geohash",
        Command::GeoSearch {
            query: GeoCommand::Search,
            ..
        } => "geosearch",
        Command::GeoSearch {
            query: GeoCommand::SearchStore,
            ..
        } => "geosearchstore",
        Command::GeoSearch {
            query: GeoCommand::Radius,
            ..
        } => "georadius",
        Command::GeoSearch {
            query: GeoCommand::RadiusByMember,
            ..
        } => "georadius",
        Command::JsonGet { .. } => "json.get",
        Command::JsonMGet { .. } => "json.mget",
        Command::JsonSet { .. } | Command::JsonSetIf { .. } => "json.set",
        Command::JsonMerge { .. } => "json.merge",
        Command::JsonDel { .. } => "json.del",
        Command::JsonOp { op, .. } => op.acl_name(),
        Command::JsonIndex { op, .. } => op.acl_name(),
        Command::BloomReserve(..) => "bf.reserve",
        Command::BloomAddMany(..) => "bf.madd",
        Command::BloomContainsMany(..) => "bf.mexists",
        Command::BloomInsert { .. } => "bf.insert",
        Command::BloomInfo(..) => "bf.info",
        Command::BloomCard(_) => "bf.card",
        Command::BloomAdd(_, _) => "bf.add",
        Command::BloomContains(_, _) => "bf.exists",
        Command::StreamAdd { .. } => "xadd",
        Command::StreamLen(_) => "xlen",
        Command::StreamInfo { .. } => "xinfo",
        Command::StreamRange { reverse: false, .. } => "xrange",
        Command::StreamRange { reverse: true, .. } => "xrevrange",
        Command::StreamRead { .. } => "xread",
        Command::StreamDelete { .. } => "xdel",
        Command::StreamTrim { .. } | Command::StreamTrimMinId { .. } => "xtrim",
        Command::StreamSetId { .. } => "xsetid",
        Command::StreamGroupCreate { .. }
        | Command::StreamGroupDestroy { .. }
        | Command::StreamGroupSetId { .. }
        | Command::StreamGroupDelConsumer { .. }
        | Command::StreamGroupCreateConsumer { .. } => "xgroup",
        Command::StreamGroupRead { .. } => "xreadgroup",
        Command::StreamAck { .. } => "xack",
        Command::StreamPending { .. } | Command::StreamPendingRange { .. } => "xpending",
        Command::StreamClaimPending { .. } => "xclaim",
        Command::StreamAutoclaim { .. } => "xautoclaim",
        Command::Expire { .. } | Command::PExpireAt { .. } => "expire",
        Command::Ttl(_) => "ttl",
        Command::PTtl(_) => "pttl",
        Command::Persist(_) => "persist",
        Command::Hello { .. } => "hello",
        Command::ClientGetName
        | Command::ClientSetName(_)
        | Command::ClientTracking(_)
        | Command::ClientDurability { .. } => "client",
        Command::Publish { .. } => "publish",
        Command::Subscribe(_) => "subscribe",
        Command::Unsubscribe(_) => "unsubscribe",
        Command::PSubscribe(_) => "psubscribe",
        Command::PUnsubscribe(_) => "punsubscribe",
        Command::ShardPublish { .. } => "spublish",
        Command::ShardSubscribe(_) => "ssubscribe",
        Command::ShardUnsubscribe(_) => "sunsubscribe",
        Command::Multi => "multi",
        Command::Exec => "exec",
        Command::Discard => "discard",
        Command::Watch(_) => "watch",
        Command::Unwatch => "unwatch",
        Command::Eval { .. } => "eval",
        Command::EvalSha { .. } => "evalsha",
        Command::FCall { .. } => "fcall",
        Command::FunctionLoad { .. } | Command::FunctionList | Command::FunctionDelete(_) => {
            "function"
        }

        Command::ScriptLoad(_)
        | Command::ScriptExists(_)
        | Command::ScriptFlush
        | Command::ScriptKill => "script",
        Command::Select(_) => "select",
        Command::FlushDb => "flushdb",
        Command::FlushAll => "flushall",
        Command::SwapDb(_, _) => "swapdb",
        Command::Move { .. } => "move",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::AclCommand;
    use std::borrow::Cow;

    fn set_user<'a>() -> Command<'a> {
        Command::Acl(AclCommand::SetUser {
            name: Cow::Borrowed(b"x"),
            rules: Vec::new(),
        })
    }

    #[test]
    fn a_rule_on_an_alias_holds_on_every_path() {
        fn parsed(frame: &[u8]) -> Command<'static> {
            let (command, _) = crate::protocol::parse_command(frame).unwrap().unwrap();

            command.into_owned()
        }

        let setex = parsed(b"*4\r\n$5\r\nSETEX\r\n$1\r\nk\r\n$2\r\n10\r\n$1\r\nv\r\n");
        let substr = parsed(b"*4\r\n$6\r\nSUBSTR\r\n$1\r\nk\r\n$1\r\n0\r\n$1\r\n1\r\n");
        let rpoplpush = parsed(b"*3\r\n$9\r\nRPOPLPUSH\r\n$1\r\na\r\n$1\r\nb\r\n");

        for (rules, command) in [
            ("on >pw +@all -setex", &setex),
            ("on >pw +@all -psetex", &setex),
            ("on >pw +@all -substr", &substr),
            ("on >pw +@all -rpoplpush", &rpoplpush),
        ] {
            let user = AclUser::parse("app", rules).unwrap();

            assert!(user.rules.authorize(command).is_err(), "{rules}");
            assert!(
                user.rules
                    .authorize_hot(&HotCommand::SetEx(b"k", b"v", 10))
                    .is_err()
                    || !rules.contains("setex"),
                "{rules}"
            );
        }

        let user = AclUser::parse("app", "on >pw +setex ~*").unwrap();

        assert_eq!(user.rules.authorize(&setex), Ok(()));

        let by_member = parsed(
            b"*5\r\n$17\r\nGEORADIUSBYMEMBER\r\n$1\r\ng\r\n$1\r\nm\r\n$1\r\n5\r\n$2\r\nkm\r\n",
        );
        let user = AclUser::parse("app", "on >pw +georadiusbymember ~*").unwrap();

        assert_eq!(user.rules.authorize(&by_member), Ok(()));
    }

    /// A subcommand rule stores the name from the static list: nothing is allocated per rule.
    #[test]
    fn a_subcommand_name_is_not_allocated() {
        let first = known_subcommand("acl|setuser").unwrap();
        let again = known_subcommand(&String::from("acl|setuser")).unwrap();

        assert!(std::ptr::eq(first, again));
        assert_eq!(first, "acl|setuser");
    }

    #[test]
    fn a_denied_subcommand_stays_denied_while_its_parent_is_allowed() {
        for rules in ["on >pw +@all -acl|setuser", "on >pw +acl -acl|setuser"] {
            let user = AclUser::parse("ops", rules).unwrap();

            assert_eq!(
                user.rules.authorize(&set_user()),
                Err(AclDeny::Command("acl|setuser")),
                "{rules}"
            );
            assert_eq!(
                user.rules.authorize(&Command::Acl(AclCommand::WhoAmI)),
                Ok(()),
                "{rules}"
            );

            // The denial survives ACL SAVE and LOAD.
            let line = user.file_line().unwrap();
            let reloaded = parse_acl_line(&line).unwrap().unwrap();

            assert!(line.contains("-acl|setuser"), "{line}");
            assert_eq!(reloaded.rules, user.rules);
            assert!(user.describe().contains("-acl|setuser"));
        }

        // `+parent` and `+parent|sub` allow it again.
        for rules in [
            "on >pw +@all -acl|setuser +acl",
            "on >pw +@all -acl|setuser +acl|setuser",
        ] {
            let user = AclUser::parse("ops", rules).unwrap();

            assert_eq!(user.rules.authorize(&set_user()), Ok(()), "{rules}");
        }

        // `-parent` drops subcommands that were allowed one by one.
        let user = AclUser::parse("ops", "on >pw +acl|setuser -acl").unwrap();

        assert!(user.rules.authorize(&set_user()).is_err());
    }

    #[test]
    fn client_kill_and_list_need_admin_rights_not_connection() {
        let kill = Command::ClientKill {
            id: Some(1),
            addr: None,
            skip_me: true,
            legacy: false,
        };
        let list = Command::ClientList { ids: Vec::new() };
        let connection = AclUser::parse("app", "on >pw ~* +@connection +get").unwrap();

        assert_eq!(
            connection
                .rules
                .authorize(&Command::ClientSetName(Cow::Borrowed(b"n"))),
            Ok(())
        );
        assert!(connection.rules.authorize(&kill).is_err());
        assert!(connection.rules.authorize(&list).is_err());
        assert!(
            connection
                .rules
                .authorize(&Command::Ruvio(Some(Cow::Borrowed(b"HOTKEYS"))))
                .is_err()
        );

        for rules in [
            "on >pw ~* +@connection +@admin",
            "on >pw ~* +@admin +@connection",
            "on >pw ~* +client",
            "on >pw ~* +@all",
        ] {
            let user = AclUser::parse("ops", rules).unwrap();

            assert_eq!(user.rules.authorize(&kill), Ok(()), "{rules}");
            assert_eq!(user.rules.authorize(&list), Ok(()), "{rules}");
        }

        let no_admin = AclUser::parse("ops", "on >pw ~* +@all -@admin").unwrap();

        assert!(no_admin.rules.authorize(&kill).is_err());
        assert_eq!(
            no_admin
                .rules
                .authorize(&Command::ClientSetName(Cow::Borrowed(b"n"))),
            Ok(())
        );
    }

    #[test]
    fn key_restricted_users_cannot_read_key_names_of_other_patterns() {
        let restricted = AclUser::parse("app", "on >pw ~app:* +@all").unwrap();
        let everyone = AclUser::parse("ops", "on >pw ~* +@all").unwrap();
        let refused = [
            Command::Changes(Some(0)),
            Command::Ruvio(Some(Cow::Borrowed(b"hotkeys"))),
            Command::Ruvio(Some(Cow::Borrowed(b"BIGKEYS"))),
            Command::Subscribe(vec![Cow::Borrowed(b"__keyspace@0__:secret")]),
            Command::Subscribe(vec![
                Cow::Borrowed(b"news"),
                Cow::Borrowed(b"__keyevent@0__:set"),
            ]),
            Command::ShardSubscribe(vec![Cow::Borrowed(b"__keyspace@0__:secret")]),
            Command::PSubscribe(vec![Cow::Borrowed(b"__keyspace@*")]),
            Command::PSubscribe(vec![Cow::Borrowed(b"*")]),
            Command::PSubscribe(vec![Cow::Borrowed(b"__key*")]),
            Command::PSubscribe(vec![Cow::Borrowed(b"_[_]keyevent@*")]),
        ];

        for command in &refused {
            assert_eq!(
                restricted.rules.authorize(command),
                Err(AclDeny::Key),
                "{command:?}"
            );
            assert_eq!(everyone.rules.authorize(command), Ok(()), "{command:?}");
        }

        for command in [
            Command::Ruvio(None),
            Command::Subscribe(vec![Cow::Borrowed(b"news")]),
            Command::PSubscribe(vec![Cow::Borrowed(b"news.*")]),
        ] {
            assert_eq!(restricted.rules.authorize(&command), Ok(()), "{command:?}");
        }
    }

    #[test]
    fn acl_file_is_written_atomically_and_owner_only() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("users.acl");
        let users = vec![AclUser::parse("app", "on >pw ~app:* +get").unwrap()];

        std::fs::write(&path, "user old on nopass\n").unwrap();
        // Writers on several shards at once each publish a whole file.
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| write_acl_file(&path, &users).unwrap());
            }
        });

        let parsed = parse_acl_file(&path).unwrap();

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].name, b"app");
        assert_eq!(
            std::fs::read_dir(directory.path()).unwrap().count(),
            1,
            "temporary files were left behind"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            assert_eq!(
                std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn user_without_password_hashes_matches_no_password() {
        let user = AclUser::parse("worker", "on ~* +@all").unwrap();

        assert!(!user.nopass);
        assert!(!user.password_matches(b""));
        assert!(!user.password_matches(b"anything"));
        assert!(
            AclUser::parse("open", "on nopass ~* +@all")
                .unwrap()
                .password_matches(b"")
        );
    }

    #[test]
    fn resetpass_on_default_user_stops_auto_login() {
        let security = SecurityConfig::default();
        assert!(ConnectionAuth::from_security(&security).authenticated);

        security
            .directory()
            .set_user(b"default", &[b"resetpass"])
            .unwrap();

        assert!(!ConnectionAuth::from_security(&security).authenticated);
        assert!(security.authenticate(None, b"").is_err());
    }

    #[test]
    fn default_user_is_nopass_until_requirepass_is_set() {
        let open = SecurityConfig::default();

        assert!(!open.authentication_enabled());
        assert!(!open.default_user().has_password());
        let locked = SecurityConfig {
            requirepass: "secret".into(),
            ..SecurityConfig::default()
        };

        assert!(locked.authentication_enabled());
        assert_eq!(
            locked.authenticate(None, b"secret").unwrap().name,
            b"default"
        );
        assert_eq!(
            locked.authenticate(None, b"wrong"),
            Err(AuthError::WrongPass)
        );
    }

    #[test]
    fn config_set_and_rewrite_can_be_denied_while_config_get_stays() {
        let user = AclUser::parse("ops", "on nopass ~* +@all -config|set").unwrap();
        let set = Command::ConfigSet(vec![(
            Cow::Borrowed(&b"maxmemory"[..]),
            Cow::Borrowed(&b"1"[..]),
        )]);

        assert_eq!(
            user.rules.authorize(&set),
            Err(AclDeny::Command("config|set"))
        );
        user.rules
            .authorize(&Command::ConfigGet(Cow::Borrowed(b"*")))
            .unwrap();
        user.rules.authorize(&Command::ConfigRewrite).unwrap();

        let viewer = AclUser::parse("viewer", "on nopass ~* +config|get").unwrap();
        viewer
            .rules
            .authorize(&Command::ConfigGet(Cow::Borrowed(b"*")))
            .unwrap();
        assert!(viewer.rules.authorize(&set).is_err());
        assert!(viewer.rules.authorize(&Command::ConfigRewrite).is_err());

        let reader = AclUser::parse("reader", "on nopass ~* +@read").unwrap();
        assert!(reader.rules.authorize(&set).is_err());
        assert!(reader.rules.authorize(&Command::ConfigRewrite).is_err());
    }

    /// A rule on a subcommand no command maps to is refused: it could never match, and each
    /// distinct name used to be kept for the life of the process (issue #38).
    #[test]
    fn unknown_subcommand_rules_are_refused() {
        let error = AclUser::parse("u", "on nopass ~* +get|anything").unwrap_err();

        assert!(
            error.contains("unknown ACL subcommand `get|anything`"),
            "{error}"
        );
        assert!(AclUser::parse("u", "on nopass ~* -config|nosuch").is_err());
        AclUser::parse("u", "on nopass ~* +config|get -client|kill").unwrap();
    }

    /// `ACL_SUBCOMMANDS` lists exactly the `parent|sub` names `command_acl_name` returns.
    #[test]
    fn the_subcommand_list_matches_command_acl_name() {
        let source = include_str!("acl.rs");
        let start = source.find("pub fn command_acl_name").unwrap();
        let body = &source[start..start + source[start..].find("\n}\n").unwrap()];
        let mut named: Vec<&str> = body
            .split('"')
            .skip(1)
            .step_by(2)
            .filter(|literal| literal.contains('|'))
            .collect();

        named.sort_unstable();
        named.dedup();
        assert_eq!(named, ACL_SUBCOMMANDS);
    }

    #[test]
    fn named_user_rules_limit_commands_and_keys() {
        let user =
            AclUser::parse("app", "on >app-secret ~app:* +@read +ping +auth -@admin").unwrap();

        assert!(user.rules.allows_command("get"));
        assert!(!user.rules.allows_command("set"));
        assert!(!user.rules.allows_command("config"));
        assert!(user.rules.allows_key(b"app:1"));
        assert!(!user.rules.allows_key(b"other"));
        user.rules
            .authorize(&Command::Get(Cow::Borrowed(b"app:1")))
            .unwrap();
        assert_eq!(
            user.rules.authorize(&Command::Get(Cow::Borrowed(b"other"))),
            Err(AclDeny::Key)
        );
        assert_eq!(
            user.rules
                .authorize(&Command::set(Cow::Borrowed(b"app:1"), Cow::Borrowed(b"v"))),
            Err(AclDeny::Command("set"))
        );
    }

    #[test]
    fn acl_whoami_and_list_do_not_echo_passwords() {
        let security = SecurityConfig {
            requirepass: "secret".into(),
            users: vec![AclUser::parse("app", "on >app-secret ~app:* +get +ping +auth").unwrap()],
            ..SecurityConfig::default()
        };

        let user = security.authenticate(Some(b"app"), b"app-secret").unwrap();
        let auth = ConnectionAuth {
            authenticated: true,
            username: user.name.clone().into(),
            rules: Arc::new(user.rules.clone()),
        };

        match acl_response(&security, &auth, &AclCommand::WhoAmI) {
            crate::engine::Response::Bulk(Some(name)) => assert_eq!(name.as_ref(), b"app"),
            other => panic!("{other:?}"),
        }

        match acl_response(&security, &auth, &AclCommand::List) {
            crate::engine::Response::Array(entries) => {
                let text = format!("{entries:?}");

                assert!(!text.contains("app-secret"));
                assert!(!text.contains("secret"));
            }

            other => panic!("{other:?}"),
        }
    }

    /// ACL SETUSER changes the shared directory. An already copied connection keeps its rules.
    #[test]
    fn setuser_updates_the_shared_directory_and_leaves_an_old_connection_alone() {
        let security = SecurityConfig::default();
        let other = security.clone();
        let connected = ConnectionAuth::from_security(&security);

        security
            .directory()
            .set_user(b"app", &[b"on", b">secret", b"~app:*", b"+get"])
            .unwrap();
        assert!(other.directory().find_user(b"app").is_some());
        let authed = security
            .directory()
            .authenticate(Some(b"app"), b"secret")
            .unwrap();

        assert!(authed.rules.allows_command("get"));
        assert!(!authed.rules.allows_command("set"));
        assert!(authed.rules.allows_key(b"app:1"));
        assert!(!authed.rules.allows_key(b"other"));
        assert!(connected.rules.allows_command("set"));
        assert!(
            security
                .directory()
                .set_user(b"bad", &[b"on", b"+notacommand"])
                .is_err()
        );
        assert!(security.directory().find_user(b"bad").is_none());
        security.directory().set_user(b"app", &[b"+set"]).unwrap();
        let again = security.authenticate(Some(b"app"), b"secret").unwrap();

        assert!(again.rules.allows_command("set"));
    }

    #[test]
    fn disabled_nopass_default_is_not_auto_authenticated() {
        let mut security = SecurityConfig::default();

        security
            .users
            .push(AclUser::parse("default", "off nopass ~* +@all").unwrap());
        let auth = ConnectionAuth::from_security(&security);

        assert!(!auth.authenticated);
    }

    #[test]
    fn acl_save_and_load_round_trip_without_touching_an_old_connection() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("users.acl");
        let security = SecurityConfig {
            acl_file: path.clone(),
            ..SecurityConfig::default()
        };

        let connected = ConnectionAuth::from_security(&security);

        security
            .directory()
            .set_user(b"app", &[b"on", b">secret", b"~app:*", b"+get"])
            .unwrap();

        match acl_response(&security, &connected, &AclCommand::Save) {
            crate::engine::Response::Simple(text) => assert_eq!(text, b"OK"),
            other => panic!("{other:?}"),
        }

        let stored = std::fs::read_to_string(&path).unwrap();

        assert!(
            !stored.contains("secret"),
            "the file keeps a hash, not the password"
        );
        assert!(stored.contains(&format!("#{}", hex_digest(&password_digest(b"secret")))));
        security.directory().set_user(b"app", &[b"+set"]).unwrap();

        match acl_response(&security, &connected, &AclCommand::Load) {
            crate::engine::Response::Simple(text) => assert_eq!(text, b"OK"),
            other => panic!("{other:?}"),
        }

        let loaded = security.directory().find_user(b"app").unwrap();

        assert!(loaded.password_matches(b"secret"));
        assert!(!loaded.password_matches(b"other"));
        assert!(loaded.rules.allows_command("get"));
        assert!(!loaded.rules.allows_command("set"));
        assert!(connected.rules.allows_command("set"));

        match acl_response(&security, &connected, &AclCommand::List) {
            crate::engine::Response::Array(entries) => {
                let text = format!("{entries:?}");

                assert!(!text.contains("secret"));
            }

            other => panic!("{other:?}"),
        }

        let unset = SecurityConfig::default();

        match acl_response(&unset, &connected, &AclCommand::Save) {
            crate::engine::Response::OwnedError(error) => {
                assert!(String::from_utf8_lossy(&error).contains("not configured"));
            }

            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn glob_supports_star_and_question() {
        assert!(glob_match(b"app:*", b"app:1"));
        assert!(glob_match(b"?", b"x"));
        assert!(!glob_match(b"?", b"xy"));
        assert!(glob_match(b"a*c", b"abc"));
        assert!(!glob_match(b"cache:*", b"app:1"));
    }
}
