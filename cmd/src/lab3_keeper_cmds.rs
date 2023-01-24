use clap::{Arg, ArgMatches, Command};
use std::io;
use std::io::Write;
use tribbler::err::{TribResult, TribblerError};

pub fn app_commands() -> [Command; 5] {
    let k = &[Arg::new("keeper").required(true)];
    let num = &[Arg::new("num keeper").required(true)];
    [
        Command::new("start").args(k),
        Command::new("start-n").args(num),
        Command::new("stop").args(k),
        Command::new("stop-n").args(num),
        Command::new("exit"),
    ]
}

pub fn repl(app: &Command) -> TribResult<ArgMatches> {
    print!("> ");
    io::stdout().flush().expect("Couldn't flush stdout");
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Error reading input.");

    let mut args = match shlex::split(&input) {
        Some(v) => v,
        None => {
            println!("error splitting args");
            return Err(Box::new(TribblerError::Unknown(
                "failed to split args".to_string(),
            )));
        }
    };
    let mut client_args = vec![app.get_name().to_string()];
    client_args.append(&mut args);
    let args = client_args;
    let matches = app.clone().try_get_matches_from(args);
    match matches {
        Ok(v) => Ok(v),
        Err(e) => {
            println!("Failed to parse args: {}", e);
            Err(Box::new(TribblerError::Unknown(
                "failed to parse args".to_string(),
            )))
        }
    }
}
