#!/usr/bin/env python3
import asyncio
import csv
from asyncua import Client, ua

class NodeCSVExporter:
    def __init__(self):
        self.nodes = []
        self.client = None
        self.aliases = {}

    async def start_node_browse(self, rootnode):
        await self.iterate_over_child_nodes(rootnode)

    async def iterate_over_child_nodes(self, node):
        self.nodes.append(node)
        for child in await node.get_children(refs=33):
            if child not in self.nodes:
                await self.iterate_over_child_nodes(child)

    async def load_aliases_from_server(self):
        datatypes_node = await self.client.nodes.root.get_child(["0:Types", "0:DataTypes"])
        await self.recursively_load_datatypes(datatypes_node)

    async def recursively_load_datatypes(self, node):
        try:
            browse_name = await node.read_browse_name()
            node_id = node.nodeid
            self.aliases[node_id.to_string()] = browse_name.Name
            for child in await node.get_children():
                await self.recursively_load_datatypes(child)
        except:
            pass

    async def export_csv(self, output_file="nodes_output.csv"):
        nodes = [node for node in self.nodes if node.nodeid.NamespaceIndex == 2]
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(["NodeId", "BrowseName", "ParentNodeId", "DataType", "DisplayName", "Description"])
            for node in nodes:
                await self.node_to_csv(node, writer)

    async def node_to_csv(self, node, writer):
        nodeid = node.nodeid.to_string()
        browsename = (await node.read_browse_name()).to_string()
        parent = await node.get_parent()
        parent_nodeid = parent.nodeid.to_string() if parent else ""
        
        if await node.read_node_class() == ua.NodeClass.Variable:
            datatype = await node.read_data_type()
            datatype_str = self.aliases.get(datatype.to_string(), datatype.to_string())
        else:
            datatype_str = ""

        displayname = (await node.read_display_name()).Text
        
        try:
            description = (await node.read_description()).Text
        except:
            description = ""

        writer.writerow([nodeid, browsename, parent_nodeid, datatype_str, displayname, description])

    async def import_nodes(self):
        self.client = Client("opc.tcp://host.docker.internal:4841")
        await self.client.connect()
        await self.load_aliases_from_server()
        root = self.client.get_root_node()
        await self.start_node_browse(root)

async def main():
    exporter = NodeCSVExporter()
    await exporter.import_nodes()
    await exporter.export_csv()
    await exporter.client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())