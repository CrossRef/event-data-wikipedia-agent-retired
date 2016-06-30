# to be run from base directory
set -e
echo "Compile JAR"
sudo -u deploy lein uberjar
echo "Install service"
cp etc/*.service /etc/systemd/system/
echo "Reload services"
systemctl daemon-reload
echo "Restart service"
service event-data-wikipedia-agent-ingest restart
service event-data-wikipedia-agent-monitor restart
service event-data-wikipedia-agent-process restart
service event-data-wikipedia-agent-push restart
echo "Done!"
