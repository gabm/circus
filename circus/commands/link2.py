import json
import subprocess
import os
from dataclasses import dataclass

from circus.commands.base import Command
from circus.exc import ArgumentError, MessageError
from pathlib import Path


def path_equal(p1, p2):
    if os.name == "nt":
        return os.path.normcase(os.path.abspath(p1)) == os.path.normcase(os.path.abspath(p2))
    else:
        return os.path.abspath(p1) == os.path.abspath(p2)


@dataclass
class Link2Tool:
    is_node: bool
    prefix: str
    environment: str
    name: str
    executable: str


class Link2(Command):

        
    """This command allows link2-specific operations: list-nodes, list-tools, add-node, add-tool

    The tools and nodes are running in their respective conda environment.
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
        self.link2_tools = list()

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

            nodes_in_env = Link2.index_link2_nodes(p, env_key)
            tools_in_env = Link2.index_well_known_tools(p, env_key)

            self.link2_tools += nodes_in_env

            for tool in tools_in_env:
                # if its not already captured by a "node"
                if not any((tool.executable == t.executable) for t in nodes_in_env):
                    self.link2_tools.append(tool)


    @staticmethod
    def index_link2_nodes(prefix, environment_name):
        result = list()

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
                        result.append(Link2Tool(
                            is_node=True,
                            executable=exe_file,
                            prefix=prefix,
                            environment=environment_name,
                            name=node_name
                        ))
            except Exception as e:
                continue

        return result

    @staticmethod
    def index_well_known_tools(prefix, environment_name):
        # this is to find tools that are not nodes
        result = list()
        allowed_prefixes = ["link2-", "ld-node-"]
        disallowed_suffixes = ["-test", ".sig"]
        exceptions = ["link2-license-tool"]

        # find nodes by specification
        bin_dir = os.path.join(prefix, "bin")
        for path in Path(bin_dir).glob("*"):
            if any(path.name.startswith(pre) for pre in allowed_prefixes) and \
                    not any(path.name == e for e in exceptions) and \
                    not any(path.name.endswith(suf) for suf in disallowed_suffixes):
                result.append(Link2Tool(
                    is_node=False,
                    executable=str(path.absolute()),
                    prefix=prefix,
                    environment=environment_name,
                    name=path.name
                ))

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
        elif cmd == "add-tool":
            response["result"] = self.handle_add_tool(arbiter, cmd_args[0], cmd_args[1], cmd_args[2], cmd_args[3])
        return response

    def handle_list_nodes(self):
        result = dict()
        for tool in self.link2_tools:
            if not tool.is_node:
                continue

            if tool.name not in result:
                result[tool.name] = list()
            result[tool.name].append(tool.environment)

        return result

    def handle_list_tools(self):
        result = dict()
        for tool in self.link2_tools:
            if tool.is_node:
                continue

            if tool.name not in result:
                result[tool.name] = list()
            result[tool.name].append(tool.environment)

        return result

    def find_node_entry(self, is_node, name, env_name):
        for tool in self.link2_tools:
            if tool.is_node != is_node:
                continue
            if tool.environment != env_name:
                continue
            if tool.name != name:
                continue
            return tool
        return None

    def handle_add_node(self, arbiter, instance_name, node, env, instance_file):
        tool = self.find_node_entry(True, node, env)

        if tool is None:
            raise MessageError("The node " + node + " in env " + env + " has not been found.")

        command = "circus_run_in_env.sh " + tool.prefix + " " + tool.executable + " --instance-file " + instance_file

        options = {
            'respawn': False,
            'autostart': False,
            'copy_env': True,
            'stop_signal': 2
        }
        watcher = arbiter.add_watcher(instance_name, command, **options)
        return "ok"

    def handle_add_tool(self, arbiter, instance_name, tool_name, env, args):
        tool = self.find_node_entry(False, tool_name , env)

        if tool is None:
            raise MessageError("The tool " + tool_name + " in env " + env + " has not been found.")

        command = "circus_run_in_env.sh " + tool.prefix + " " + tool.executable + " " + args

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

        if cmd not in ["list-nodes", "list-tools", "add-node", "add-tool"]:
            raise ArgumentError("Unknown command " + cmd)

        if cmd.startswith("list-"):
            if len(cmd_args) != 0:
                raise ArgumentError("the list commands don't take arguments")
        elif cmd == "add-node":
            if len(cmd_args) != 4:
                raise ArgumentError("The add-node command takes four arguments (add-node <instance-name> <node-name> <env-name> <instance-file>)")
        elif cmd == "add-tool":
            if len(cmd_args) != 4:
                raise ArgumentError(
                    "The add-node command takes four arguments (add-tool <instance-name> <tool-name> <env-name> [args])")
