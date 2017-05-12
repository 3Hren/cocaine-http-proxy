use std::iter;
use std::net::SocketAddr;
use std::result::Result;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use console::{self, Term};

use itertools::Itertools;

use cocaine::{Builder, Core, Error, FixedResolver};

use Config;

pub fn run_dashboard(cfg: Config) -> JoinHandle<Result<(), Error>> {
    thread::spawn(move || {
        let locator_addrs = cfg.locators()
            .iter()
            .map(|&(addr, port)| SocketAddr::new(addr, port))
            .collect::<Vec<SocketAddr>>();

        let mut core = Core::new()?;
        let locator = Builder::new("locator")
            .resolver(FixedResolver::new(locator_addrs.clone()))
            .build(&core.handle());

        let logging_common = Builder::new(cfg.logging().common().name().to_owned())
            .locator_addrs(locator_addrs.clone())
            .build(&core.handle());

        let logging_access = Builder::new(cfg.logging().access().name().to_owned())
            .locator_addrs(locator_addrs.clone())
            .build(&core.handle());

        let unicorn = Builder::new("unicorn")
            .locator_addrs(locator_addrs.clone())
            .build(&core.handle());

        let term = Term::stdout();

        loop {
            let (.., width) = term.size();

            let head = "Cocaine HTTP proxy dashboard";

            struct Status {
                locator: Result<(), Error>,
                logging_common: Result<(), Error>,
                logging_access: Result<(), Error>,
                unicorn: Result<(), Error>,
            }

            let status = Status {
                locator: core.run(locator.connect()),
                logging_common: core.run(logging_common.connect()),
                logging_access: core.run(logging_access.connect()),
                unicorn: core.run(unicorn.connect()),
            };

            let lines = &[
                &format!(
                    "{} {} {}",
                    console::style(iter::repeat('―').take(4).collect::<String>()).bold(),
                    console::style(head).bold().yellow(),
                    console::style(iter::repeat('―').take((width as usize).saturating_sub(6 + head.len())).collect::<String>()).bold()
                ),
                &format!("{} {}", console::style("Server").bold(), console::style(&cfg.network().addr()).white()),
                &format!("{} {}", console::style("Monitoring").bold(), console::style(&cfg.monitoring().addr()).white()),
                "",
                &format!("For more information about monitoring API visit {}", console::style(format!("http://{}", &cfg.monitoring().addr())).white()),
                "",
                &format!(
                    "{} {:54} [{}]",
                    console::style("Locator").bold(),
                    console::style(locator_addrs.iter().join(", ")).white(),
                    if status.locator.is_ok() { console::style("✓").green() } else { console::style("✗").red() },
                ),
                &format!(
                    "{} {:54} [{}]",
                    console::style("Logging").bold(),
                    console::style("logging").white(),
                    if status.logging_common.is_ok() { console::style("✓").green() } else { console::style("✗").red() },
                ),
                &format!(
                    "{} {:54} [{}]",
                    console::style("       ").bold(),
                    console::style("logging-access").white(),
                    if status.logging_access.is_ok() { console::style("✓").green() } else { console::style("✗").red() },
                ),
                &format!(
                    "{} {:54} [{}]",
                    console::style("Unicorn").bold(),
                    console::style("unicorn").white(),
                    if status.unicorn.is_ok() { console::style("✓").green() } else { console::style("✗").red() },
                ),
                &format!(
                    "{}",
                    console::style(iter::repeat('―').take(width as usize).collect::<String>()).bold()
                ),
            ];

            for line in &lines[..] {
                term.write_line(line)?;
            }

            thread::sleep(Duration::from_millis(500));
            term.clear_last_lines(lines.len())?;
        }
    })
}
