import copy

import pytest

from ray.autoscaler._private.aws.config import _get_vpc_id_or_die, \
    bootstrap_aws, log_to_cli, \
    DEFAULT_AMI
import ray.tests.aws.utils.stubs as stubs
import ray.tests.aws.utils.helpers as helpers
from ray.tests.aws.utils.constants import AUX_SUBNET, DEFAULT_SUBNET, \
    DEFAULT_SG_AUX_SUBNET, DEFAULT_SG, DEFAULT_SG_DUAL_GROUP_RULES, \
    DEFAULT_SG_WITH_RULES_AUX_SUBNET, AUX_SG, \
    DEFAULT_SG_WITH_RULES, DEFAULT_SG_WITH_NAME, \
    DEFAULT_SG_WITH_NAME_AND_RULES, CUSTOM_IN_BOUND_RULES, \
    DEFAULT_KEY_PAIR


def test_use_subnets_in_only_one_vpc(iam_client_stub, ec2_client_stub):
    """
    This test validates that when bootstrap_aws populates the SubnetIds field,
    all of the subnets used belong to the same VPC, and that a SecurityGroup
    in that VPC is correctly configured.
    """
    stubs.configure_iam_role_default(iam_client_stub)
    stubs.configure_key_pair_default(ec2_client_stub)

    # Add a response with a thousand subnets all in different VPCs.
    # After filtering, only subnet in one particular VPC should remain.
    # Thus SubnetIds for each available node type should end up as
    # being length-one lists after the bootstrap_config.
    stubs.describe_a_thousand_subnets_in_different_vpcs(ec2_client_stub)

    # describe the subnet in use while determining its vpc
    stubs.describe_subnets_echo(ec2_client_stub, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    stubs.describe_no_security_groups(ec2_client_stub)
    # expect to create a security group on the VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG)
    # expect new security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [DEFAULT_SUBNET["VpcId"]],
        [DEFAULT_SG],
    )

    # given no existing default security group inbound rules...
    # expect to authorize all default inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_WITH_RULES,
    )

    # expect another call to describe the above security group while checking
    # a second time if it has ip_permissions set ("if not sg.ip_permissions")
    stubs.describe_an_sg_2(
        ec2_client_stub,
        DEFAULT_SG_WITH_RULES,
    )

    # given our mocks and an example config file as input...
    # expect the config to be loaded, validated, and bootstrapped successfully
    config = helpers.bootstrap_aws_example_config_file("example-full.yaml")
    _get_vpc_id_or_die.cache_clear()

    # We've filtered down to only one subnet id -- only one of the thousand
    # subnets generated by ec2.subnets.all() belongs to the right VPC.
    for node_type in config["available_node_types"]:
        node_config = config["available_node_types"][node_type]["node_config"]
        assert node_config["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]
        assert node_config["SecurityGroupIds"] == [DEFAULT_SG["GroupId"]]


def test_create_sg_different_vpc_same_rules(iam_client_stub, ec2_client_stub):
    # use default stubs to skip ahead to security group configuration
    stubs.skip_to_configure_sg(ec2_client_stub, iam_client_stub)

    # given head and worker nodes with custom subnets defined...
    # expect to first describe the worker subnet ID
    stubs.describe_subnets_echo(ec2_client_stub, AUX_SUBNET)
    # expect to second describe the head subnet ID
    stubs.describe_subnets_echo(ec2_client_stub, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    stubs.describe_no_security_groups(ec2_client_stub)
    # expect to first create a security group on the worker node VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG_AUX_SUBNET)
    # expect new worker security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [AUX_SUBNET["VpcId"]],
        [DEFAULT_SG_AUX_SUBNET],
    )
    # expect to second create a security group on the head node VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG)
    # expect new head security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [DEFAULT_SUBNET["VpcId"]],
        [DEFAULT_SG],
    )

    # given no existing default head security group inbound rules...
    # expect to authorize all default head inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_DUAL_GROUP_RULES,
    )
    # given no existing default worker security group inbound rules...
    # expect to authorize all default worker inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_WITH_RULES_AUX_SUBNET,
    )

    # given our mocks and an example config file as input...
    # expect the config to be loaded, validated, and bootstrapped successfully
    config = helpers.bootstrap_aws_example_config_file("example-subnets.yaml")

    # expect the bootstrapped config to show different head and worker security
    # groups residing on different subnets
    for node_type in config["available_node_types"]:
        node_config = config["available_node_types"][node_type]["node_config"]
        security_group_ids = node_config["SecurityGroupIds"]
        subnet_ids = node_config["SubnetIds"]
        if node_type == config["head_node_type"]:
            assert security_group_ids == [DEFAULT_SG["GroupId"]]
            assert subnet_ids == [DEFAULT_SUBNET["SubnetId"]]
        else:
            assert security_group_ids == [AUX_SG["GroupId"]]
            assert subnet_ids == [AUX_SUBNET["SubnetId"]]

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


def test_create_sg_with_custom_inbound_rules_and_name(iam_client_stub,
                                                      ec2_client_stub):
    # use default stubs to skip ahead to security group configuration
    stubs.skip_to_configure_sg(ec2_client_stub, iam_client_stub)

    # expect to describe the head subnet ID
    stubs.describe_subnets_echo(ec2_client_stub, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    stubs.describe_no_security_groups(ec2_client_stub)
    # expect to create a security group on the head node VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_NAME)
    # expect new head security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [DEFAULT_SUBNET["VpcId"]],
        [DEFAULT_SG_WITH_NAME],
    )

    # given custom existing default head security group inbound rules...
    # expect to authorize both default and custom inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_WITH_NAME_AND_RULES,
    )

    # given the prior modification to the head security group...
    # expect the next read of a head security group property to reload it
    stubs.describe_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_NAME_AND_RULES)

    _get_vpc_id_or_die.cache_clear()
    # given our mocks and an example config file as input...
    # expect the config to be loaded, validated, and bootstrapped successfully
    config = helpers.bootstrap_aws_example_config_file(
        "example-security-group.yaml")

    # expect the bootstrapped config to have the custom security group...
    # name and in bound rules
    assert config["provider"]["security_group"][
        "GroupName"] == DEFAULT_SG_WITH_NAME_AND_RULES["GroupName"]
    assert config["provider"]["security_group"][
        "IpPermissions"] == CUSTOM_IN_BOUND_RULES

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


def test_subnet_given_head_and_worker_sg(iam_client_stub, ec2_client_stub):
    stubs.configure_iam_role_default(iam_client_stub)
    stubs.configure_key_pair_default(ec2_client_stub)

    # list a security group and a thousand subnets in different vpcs
    stubs.describe_a_security_group(ec2_client_stub, DEFAULT_SG)
    stubs.describe_a_thousand_subnets_in_different_vpcs(ec2_client_stub)

    config = helpers.bootstrap_aws_example_config_file(
        "example-head-and-worker-security-group.yaml")

    # check that just the single subnet in the right vpc is filled
    for node_type in config["available_node_types"]:
        node_config = config["available_node_types"][node_type]["node_config"]
        assert node_config["SubnetIds"] == [DEFAULT_SUBNET["SubnetId"]]

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


def test_fills_out_amis(iam_client_stub, ec2_client_stub):
    # Setup stubs to mock out boto3
    stubs.configure_iam_role_default(iam_client_stub)
    stubs.configure_key_pair_default(ec2_client_stub)
    stubs.describe_a_security_group(ec2_client_stub, DEFAULT_SG)
    stubs.configure_subnet_default(ec2_client_stub)

    config = helpers.load_aws_example_config_file("example-full.yaml")
    head_node_config = config["available_node_types"]["ray.head.default"][
        "node_config"]
    worker_node_config = config["available_node_types"]["ray.worker.default"][
        "node_config"]

    del head_node_config["ImageId"]
    del worker_node_config["ImageId"]

    # Pass in SG for stub to work
    head_node_config["SecurityGroupIds"] = ["sg-1234abcd"]
    worker_node_config["SecurityGroupIds"] = ["sg-1234abcd"]

    defaults_filled = bootstrap_aws(config)

    ami = DEFAULT_AMI.get(config.get("provider", {}).get("region"))

    for node_type in defaults_filled["available_node_types"].values():
        node_config = node_type["node_config"]
        assert node_config.get("ImageId") == ami

    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


def test_create_sg_multinode(iam_client_stub, ec2_client_stub):
    """
    Test AWS Bootstrap logic when config being bootstrapped has the
    following properties:

    (1) auth config does not specify ssh key path
    (2) available_node_types is provided
    (3) security group name and ip permissions set in provider field
    (4) Available node types have SubnetIds field set and this
        field is of form SubnetIds: [subnet-xxxxx].
        Both node types specify the same subnet-xxxxx.

    Tests creation of a security group and key pair under these conditions.
    """

    # Generate a config of the desired form.
    subnet_id = DEFAULT_SUBNET["SubnetId"]

    # security group info to go in provider field
    provider_data = helpers.load_aws_example_config_file(
        "example-security-group.yaml")["provider"]

    # a multi-node-type config -- will add head/worker stuff and security group
    # info to this.
    base_config = helpers.load_aws_example_config_file("example-full.yaml")

    config = copy.deepcopy(base_config)
    # Add security group data
    config["provider"] = provider_data
    # Add head and worker fields.
    head_node_config = config["available_node_types"]["ray.head.default"][
        "node_config"]
    worker_node_config = config["available_node_types"]["ray.worker.default"][
        "node_config"]
    head_node_config["SubnetIds"] = [subnet_id]
    worker_node_config["SubnetIds"] = [subnet_id]

    # Generate stubs
    stubs.configure_iam_role_default(iam_client_stub)
    stubs.configure_key_pair_default(ec2_client_stub)

    # Only one of these (the one specified in the available_node_types)
    # is in the correct vpc.
    # This list of subnets is generated by the ec2.subnets.all() call
    # and then ignored, since available_node_types already specify
    # subnet_ids.
    stubs.describe_a_thousand_subnets_in_different_vpcs(ec2_client_stub)

    # The rest of the stubbing logic is copied from
    # test_create_sg_with_custom_inbound_rules_and_name.

    # expect to describe the head subnet ID
    stubs.describe_subnets_echo(ec2_client_stub, DEFAULT_SUBNET)
    # given no existing security groups within the VPC...
    stubs.describe_no_security_groups(ec2_client_stub)
    # expect to create a security group on the head node VPC
    stubs.create_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_NAME)
    # expect new head security group details to be retrieved after creation
    stubs.describe_sgs_on_vpc(
        ec2_client_stub,
        [DEFAULT_SUBNET["VpcId"]],
        [DEFAULT_SG_WITH_NAME],
    )

    # given custom existing default head security group inbound rules...
    # expect to authorize both default and custom inbound rules
    stubs.authorize_sg_ingress(
        ec2_client_stub,
        DEFAULT_SG_WITH_NAME_AND_RULES,
    )

    # given the prior modification to the head security group...
    # expect the next read of a head security group property to reload it
    stubs.describe_sg_echo(ec2_client_stub, DEFAULT_SG_WITH_NAME_AND_RULES)

    _get_vpc_id_or_die.cache_clear()

    # given our mocks and the config as input...
    # expect the config to be validated and bootstrapped successfully
    bootstrapped_config = helpers.bootstrap_aws_config(config)

    # expect the bootstrapped config to have the custom security group...
    # name and in bound rules
    assert bootstrapped_config["provider"]["security_group"][
        "GroupName"] == DEFAULT_SG_WITH_NAME_AND_RULES["GroupName"]
    assert config["provider"]["security_group"][
        "IpPermissions"] == CUSTOM_IN_BOUND_RULES

    # Confirming correct security group got filled for head and workers
    sg_id = DEFAULT_SG["GroupId"]
    for node_type in bootstrapped_config["available_node_types"].values():
        assert node_type["node_config"]["SecurityGroupIds"] == [sg_id]

    # Confirming boostrap config updates available node types with
    # default KeyName
    for node_type in bootstrapped_config["available_node_types"].values():
        node_config = node_type["node_config"]
        assert node_config["KeyName"] == DEFAULT_KEY_PAIR["KeyName"]

    # Confirm security group is in the right VPC.
    # (Doesn"t really confirm anything except for the structure of this test
    # data.)
    bootstrapped_head_type = bootstrapped_config["head_node_type"]
    bootstrapped_types = bootstrapped_config["available_node_types"]
    bootstrapped_head_config = bootstrapped_types[bootstrapped_head_type][
        "node_config"]
    assert DEFAULT_SG["VpcId"] == DEFAULT_SUBNET["VpcId"]
    assert DEFAULT_SUBNET["SubnetId"] == bootstrapped_head_config["SubnetIds"][
        0]

    # ssh private key filled in
    assert "ssh_private_key" in bootstrapped_config["auth"]

    # expect no pending responses left in IAM or EC2 client stub queues
    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


def test_missing_keyname(iam_client_stub, ec2_client_stub):
    config = helpers.load_aws_example_config_file("example-full.yaml")
    config["auth"]["ssh_private_key"] = "/path/to/private/key"
    head_node_config = config["available_node_types"]["ray.head.default"][
        "node_config"]
    worker_node_config = config["available_node_types"]["ray.worker.default"][
        "node_config"]

    # Setup stubs to mock out boto3. Should fail on assertion after
    # checking KeyName/UserData.
    stubs.configure_iam_role_default(iam_client_stub)

    missing_user_data_config = copy.deepcopy(config)
    with pytest.raises(AssertionError):
        # Config specified ssh_private_key, but missing KeyName/UserData in
        # node configs
        bootstrap_aws(missing_user_data_config)

    # Stubs to mock out boto3. Should no longer fail on assertion
    # and go on to describe security groups + configure subnet
    stubs.configure_iam_role_default(iam_client_stub)
    stubs.describe_a_security_group(ec2_client_stub, DEFAULT_SG)
    stubs.configure_subnet_default(ec2_client_stub)

    # Pass in SG for stub to work
    head_node_config["SecurityGroupIds"] = ["sg-1234abcd"]
    worker_node_config["SecurityGroupIds"] = ["sg-1234abcd"]

    # Set UserData for both node configs
    head_node_config["UserData"] = {"someKey": "someValue"}
    worker_node_config["UserData"] = {"someKey": "someValue"}

    # Should work without error now that UserData is set
    bootstrap_aws(config)

    iam_client_stub.assert_no_pending_responses()
    ec2_client_stub.assert_no_pending_responses()


def test_log_to_cli(iam_client_stub, ec2_client_stub):
    config = helpers.load_aws_example_config_file("example-full.yaml")

    head_node_config = config["available_node_types"]["ray.head.default"][
        "node_config"]
    worker_node_config = config["available_node_types"]["ray.worker.default"][
        "node_config"]

    # Pass in SG for stub to work
    head_node_config["SecurityGroupIds"] = ["sg-1234abcd"]
    worker_node_config["SecurityGroupIds"] = ["sg-1234abcd"]

    stubs.configure_iam_role_default(iam_client_stub)
    stubs.configure_key_pair_default(ec2_client_stub)
    stubs.describe_a_security_group(ec2_client_stub, DEFAULT_SG)
    stubs.configure_subnet_default(ec2_client_stub)

    config = helpers.bootstrap_aws_config(config)

    # Only side-effect is to generate logs, just checking that works without
    # error
    log_to_cli(config)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
