DATABASE_URL=sqlite://builds.db sqlx database create
DATABASE_URL=sqlite://builds.db sqlx migrate run
DATABASE_URL=sqlite://builds.db cargo build --release
