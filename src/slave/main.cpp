/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <libgen.h> // For chdir.

#include "common/build.hpp"
#include "common/logging.hpp"
#include "common/try.hpp"

#include "configurator/configurator.hpp"

#include "detector/detector.hpp"

#include "isolation_module_factory.hpp"
#include "slave.hpp"
#include "webui.hpp"

using namespace mesos::internal;
using namespace mesos::internal::slave;

using std::cerr;
using std::endl;
using std::string;


void usage(const char *programName, const Configurator& configurator)
{
  cerr << "Usage: " << programName << " [...]" << endl
       << endl
       << endl
       << "Supported options:" << endl
       << configurator.getUsage();
}


int main(int argc, char** argv)
{
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  Configurator configurator;
  Logging::registerOptions(&configurator);
  Slave::registerOptions(&configurator);

  // The following options are executable specific (e.g., since we
  // only have one instance of libprocess per execution, we only want
  // to advertise the port and ip option once, here).
  configurator.addOption<int>("port", 'p', "Port to listen on", 0);
  configurator.addOption<string>("ip", "IP address to listen on");
  configurator.addOption<string>("isolation", "Isolation module", "process");
#ifdef MESOS_WEBUI
  configurator.addOption<int>("webui_port", "Web UI port", 8081);
#endif
  configurator.addOption<string>(
      "master",
      'm',
      "May be one of:\n"
      "  host:port\n"
      "  zk://host1:port1,host2:port2,.../path\n"
      "  zk://username:password@host1:port1,host2:port2,.../path\n"
      "  file://path/to/file (where file contains one of the above)");

  if (argc == 2 && string("--help") == argv[1]) {
    usage(argv[0], configurator);
    exit(1);
  }

  Configuration conf;
  try {
    conf = configurator.load(argc, argv);
  } catch (ConfigurationException& e) {
    cerr << "Configuration error: " << e.what() << endl;
    exit(1);
  }

  Logging::init(argv[0], conf);

  if (conf.contains("port")) {
    setenv("LIBPROCESS_PORT", conf["port"].c_str(), 1);
  }

  if (conf.contains("ip")) {
    setenv("LIBPROCESS_IP", conf["ip"].c_str(), 1);
  }

  // Initialize libprocess library (but not glog, done above).
  process::initialize(false);

  if (!conf.contains("master")) {
    cerr << "Missing required option --master (-m)" << endl;
    exit(1);
  }

  string master = conf["master"];

  string isolation = conf["isolation"];
  LOG(INFO) << "Creating \"" << isolation << "\" isolation module";
  IsolationModule* isolationModule = IsolationModule::create(isolation);

  if (isolationModule == NULL) {
    cerr << "Unrecognized isolation type: " << isolation << endl;
    exit(1);
  }

  LOG(INFO) << "Build: " << build::DATE << " by " << build::USER;
  LOG(INFO) << "Starting Mesos slave";

  if (chdir(dirname(argv[0])) != 0) {
    fatalerror("Could not chdir into %s", dirname(argv[0]));
  }

  Slave* slave = new Slave(conf, false, isolationModule);
  process::spawn(slave);

  Try<MasterDetector*> detector =
    MasterDetector::create(master, slave->self(), false, Logging::isQuiet(conf));

  CHECK(detector.isSome())
    << "Failed to create a master detector: " << detector.error();

#ifdef MESOS_WEBUI
  webui::start(slave->self(), conf);
#endif

  process::wait(slave->self());
  delete slave;

  MasterDetector::destroy(detector.get());
  IsolationModule::destroy(isolationModule);

  return 0;
}
