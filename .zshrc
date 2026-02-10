# Swarm Manager - Override DATABASE_URL when in swarm dir
if [ -f .env ]; then
  export DATABASE_URL="$(grep DATABASE_URL .env | cut -d= -f2-)"
fi
