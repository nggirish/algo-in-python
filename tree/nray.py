end_of_child_mark = "#"

class NTree():
    def __init__(self,sval,child=None):
        self.val = sval
        if not child:
            self.child = []


class RTree:
    node = None


def deserialize_r(root,ix,data):
    val = data.pop(0)
    if not data or val == end_of_child_mark:
        return True

    if root == '':
        RTree.node = NTree(val)
    else:
        eval("RTree.node" + root + ".append(NTree(val))")

    if root.endswith("child"):
        root += "[%s]" % ix
    root += ".child"
    ix=0
    while 1:
        if deserialize_r(root,ix,data):
            break
        ix += 1
    return False


def deserialize(data=None):
    ntree =[]
    while data:
        d = data.pop(0)
        if d == end_of_child_mark:
            root=ntree.pop()
            continue
        if ntree:
            root=ntree[-1]
            root.child.append(NTree(d))
            ntree.append(root.child[-1])
        else:
            ntree.append(NTree(d))
    return root


def serialize_r(root):
    ret_val = []

    def encode(root):
        if not root:
            return

        ret_val.append(root.val)
        for node in root.child:
            encode(node)

        ret_val.append(end_of_child_mark)

    encode(root)
    return ret_val


def serialize(root):
    node = [root,end_of_child_mark]
    ret_val = []
    while node:
        n = node.pop(0)

        if isinstance(n,str):
            ret_val.append(n)
            continue

        ret_val.append(n.val)

        t_n = []
        for c in n.child:
            t_n += [c, end_of_child_mark]
        node = t_n + node
    return ret_val


def traverse(root):

    print(root.val)

    for node in root.child:
        traverse(node)


def main():
    ntree = NTree('A')
    ntree.child.append(NTree('B'))
    ntree.child.append(NTree('C'))
    ntree.child.append(NTree('D'))
    ntree.child[0].child.append(NTree('E'))
    ntree.child[0].child.append(NTree('F'))
    ntree.child[2].child.append(NTree('G'))
    ntree.child[2].child.append(NTree('H'))
    ntree.child[2].child.append(NTree('I'))
    ntree.child[2].child.append(NTree('J'))
    ntree.child[0].child[1].child.append(NTree('K'))

    print("Serialize Tree")
    data = serialize(ntree)
    print(data)

    print("De Serialize Tree")
    ntree = deserialize(data)
    traverse(ntree)

    print("Serialize Tree Recursive")
    data = serialize_r(ntree)
    print(data)

    print("De Serialize Tree Recursive")
    deserialize_r('',None,data)
    traverse(RTree.node)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        raise e
