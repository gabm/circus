import json
import subprocess


from circus.commands.base import Command
from circus.exc import ArgumentError, MessageError
from circus import logger
import os
from pathlib import Path


def path_equal(p1, p2):
    if os.name == "nt":
        return os.path.normcase(os.path.abspath(p1)) == os.path.normcase(os.path.abspath(p2))
    else:
        return os.path.abspath(p1) == os.path.abspath(p2)


class Link2(Command):

        
    """It is a good practice to describe what the class does here.

    Have a look at other commands to see how we are used to format
    this text. It will be automatically included in the documentation,
    so don't be afraid of being exhaustive, that's what it is made
    for.
    """
    # all the commands inherit from `circus.commands.base.Command`

    # you need to specify a name so we find back the command somehow
    name = "link2"

    # Set waiting to True or False to define your default behavior
    # - If waiting is True, the command is run synchronously, and the client may get
    #   back results.
    # - If waiting is False, the command is run asynchronously on the server and the client immediately
    #   gets back an 'ok' response
    #
    #   By default, commands are set to waiting = False
    waiting = True

    # options
    options = []

    properties = ['subcommand', 'subcommand-args']
    # properties list the command arguments that are mandatory. If they are
    # not provided, then an error will be thrown

    def __init__(self):
        super(Link2, self).__init__()
        self.link2_nodes = dict()
        self.well_known_tools = dict()
        self.all_prefixes = dict()

        ret = subprocess.run(["conda", "info", "--json"], capture_output=True, encoding="utf-8")
        if ret.returncode != 0:
            return

        conda_info = json.loads(ret.stdout)
        conda_prefixes = conda_info["envs"]
        conda_envs_dirs = conda_info["envs_dirs"]
        root_prefix = conda_info["root_prefix"]

        for p in conda_prefixes:
            if p == root_prefix:
                env_key = "base"
            elif any(path_equal(envs_dir,os.path.dirname(p)) for envs_dir in conda_envs_dirs):
                env_key = os.path.basename(p)
            else:
                # skip if its not in a standard env dir
                continue

            self.all_prefixes[env_key] = p
            nodes_in_env = Link2.index_link2_nodes(p)
            tools_in_env = Link2.index_well_known_tools(p)

            if len(nodes_in_env) > 0:
                self.link2_nodes[env_key] = nodes_in_env

            for (tool, path) in tools_in_env.items():
                # if its not already captured by a "node"
                if not any((path == p) for (_, p) in nodes_in_env.items()):
                    if env_key not in self.well_known_tools:
                        self.well_known_tools[env_key] = dict()
                    self.well_known_tools[env_key][tool] = path

        print(self.link2_nodes)
        print(self.well_known_tools)

    @staticmethod
    def index_link2_nodes(prefix):
        result = dict()

        # find nodes by specification
        share_dir = os.path.join(prefix, "share", "link2", "static_assets")
        bin_dir = os.path.join(prefix, "bin")
        for path in Path(share_dir).rglob("*.json"):
            try:
                with open(path.absolute(), 'r') as f:
                    spec = json.load(f)
                    id = spec["$id"]
                    tokens = id.split('/')

                    # last element is the node name
                    # we assume that this is equal to the executable name
                    # but thats not always true! As of now there is no way to find that out :/
                    node_name = tokens[-1]

                    exe_file = os.path.join(bin_dir, node_name)
                    if os.path.isfile(exe_file):
                        result[node_name] = exe_file
            except Exception as e:
                continue

        return result

    @staticmethod
    def index_well_known_tools(prefix):
        # this is to find tools that are not nodes
        result = dict()
        allowed_prefixes = ["link2-", "ld-node-"]
        disallowed_suffixes = ["-test", ".sig"]
        exceptions = ["link2-license-tool"]

        # find nodes by specification
        bin_dir = os.path.join(prefix, "bin")
        for path in Path(bin_dir).glob("*"):
            if any(path.name.startswith(pre) for pre in allowed_prefixes) and \
                    not any(path.name == e for e in exceptions) and \
                    not any(path.name.endswith(suf) for suf in disallowed_suffixes):
                result[path.name] = str(path.absolute())

        return result

    def execute(self, arbiter, props):
        cmd = props.get("subcommand")
        cmd_args = props.get("subcommand-args")

        response = dict()
        response["subcommand"] = cmd
        response["result"] = None

        if cmd == "list-nodes":
            response["result"] = self.handle_list_nodes()
        elif cmd == "list-tools":
            response["result"] = self.handle_list_tools()
        elif cmd == "add-node":
            response["result"] = self.handle_add_node(arbiter, cmd_args[0], cmd_args[1], cmd_args[2], cmd_args[3])
        return response

    def handle_list_nodes(self):
        result = dict()
        for env, nodes in self.link2_nodes.items():
            for (name, _) in nodes.items():
                if name not in result:
                    result[name] = list()
                result[name].append(env)
        return result

    def handle_list_tools(self):
        result = dict()
        for env, tools in self.well_known_tools.items():
            for (name, _) in tools.items():
                if name not in result:
                    result[name] = list()
                result[name].append(env)
        return result

    def handle_add_node(self, arbiter, instance_name, node, env, instance_file):
        if env not in self.link2_nodes:
            raise MessageError(env + " is not a valid conda env")

        node_env = self.link2_nodes.get(env)
        if node not in node_env:
            raise MessageError(node + " is not a valid node in env " + env)

        command = "/home/gabm/dev/os/circus/run_in.sh " + "/home/gabm/miniconda3/envs/" + env + " " + node + " --instance-file " + instance_file
        print("Command to run: " + command)

        options = {
            'respawn': False,
            'autostart': False,
            'copy_env': True,
            'stop_signal': 2
        }
        watcher = arbiter.add_watcher(instance_name, command, **options)
        return "ok"



    def console_msg(self, msg):
        # msg is what is returned by the execute method.
        # this method is used to format the response for a console (it is
        # used for instance by circusctl to print its messages)
        print(msg)

    def message(self, *args, **opts):
        # message handles console input.
        # this method is used to map console arguments to the command
        # options. (its is used for instance when calling the command via
        # circusctl)
        # NotImplementedError will be thrown if the function is missing
        if len(args) < 1:
            raise ArgumentError('Invalid number of arguments.')
        else:
            opts['subcommand'] = args[0]
            opts['subcommand-args'] = args[1:] if len(args) > 2 else list()
        return self.make_message(**opts)

    def validate(self, props):
        # this method is used to validate that the arguments passed to the
        # command are correct. An ArgumentError should be thrown in case
        # there is an error in the passed arguments (for instance if they
        # do not match together.
        # In case there is a problem wrt their content, a MessageError
        # should be thrown. This method can modify the content of the props
        # dict, it will be passed to execute afterwards.
        cmd = props.get("subcommand")
        cmd_args = props.get("subcommand-args")

        if cmd not in ["list-nodes", "list-tools", "add-node"]:
            raise ArgumentError("Unknown command " + cmd)

        if cmd.startswith("list-"):
            if len(cmd_args) != 0:
                raise ArgumentError("the list commands don't take arguments")
        elif cmd == "add-node":
            if len(cmd_args) != 4:
                raise ArgumentError("The add-node command takes three arguments (add-node <instance-name> <node-name> <env-name> <instance-file>)")
